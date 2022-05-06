
const RedisClient          = require('@kirick/redis-client/src/client');
const { objectToHashBulk,
        hashBulkToObject } = require('@kirick/redis-client/src/tools');
const OhMyProps            = require('oh-my-props');

const { REDIS_PREFIX,
        REDIS_SUBKEY_INCR,
        REDIS_SUBKEY_INSERT,
        REDIS_SUBKEY_DOCUMENT_IDS,
        REDIS_SUBKEY_INDEX       } = require('./consts');
const { hasOwnProperty,
        isPlainObject,
        unixtime,
        getID,
        getElement,
        parseHashedValue }         = require('./fns');
const INDEX                        = require('./indexes');
const createScripts                = require('./scripts');
const Timing                       = require('./timing');
const TemporaryKeys                = require('./temporary-keys');

const schemaProps = new OhMyProps({
    namespace: {
        type: String,
        validator: (value) => value.length > 0,
    },
    auto_document_id: {
        type: Boolean,
        default: true,
    },
    index: {
        type: Array,
        default: () => [],
        subvalidator: new OhMyProps({
            field: {
                type: String,
                validator: (value) => value.length > 0,
            },
            type: {
                type: Number,
                validator: (value) => INDEX._values.has(value),
            },
        }),
    },
    document_schema: {
        type: OhMyProps,
        is_nullable: true,
        default: null,
    },
});

class RedisStore {
    constructor (
        redisClient,
        schema = {},
    ) {
        if (redisClient instanceof RedisClient !== true) {
            throw new TypeError('Argument 0 must be an instance of RedisClient.');
        }
        this.redisClient = redisClient;

        this.schema = schemaProps.transform(schema);
        if (null === this.schema) {
            throw new TypeError('Argument 1 contains invalid schema.');
        }

        this._indexes_by_field = new Map();
        this._indexes_by_type = new Map();
        for (const { field, type } of this.schema.index) {
            this._indexes_by_field.set(field, type);

            if (this._indexes_by_type.has(type) === false) {
                this._indexes_by_type.set(type, new Set());
            }
            this._indexes_by_type.get(type).add(field);
        }

        this._redis_prefix = `${REDIS_PREFIX}:${this.schema.namespace}`;

        this._ts_delete_expired = 0;
    }

    async _loadScripts () {
        if (hasOwnProperty(this, '_scripts') !== true) {
            this._scripts = await createScripts(this);
            // console.log(this._scripts);
        }
    }

    /* async */ insertOne (document) {
        return this.insertMany([
            document,
        ]);
    }

    async insertMany (documents) {
        if (documents.length > 0) {
            const insert_op_id = getID();

            const is_document_id_auto = this.schema.auto_document_id;

            let document_id_increment;
            if (is_document_id_auto) {
                document_id_increment = (await this.redisClient.INCRBY(`${this._redis_prefix}:${REDIS_SUBKEY_INCR}`, documents.length)) - documents.length;
            }

            const REDIS_KEY_INSERT = `${this._redis_prefix}:${REDIS_SUBKEY_INSERT}`;

            const multi = this.redisClient.MULTI();

            await this._deleteExpired(multi);
            multi.as('expired');

            // check if there are unique index duplicates
            {
                const unique_check_document_ids = [];
                const unique_check_fields = [];

                const unique_fields_new = {};
                for (const field of this._indexes_by_type.get(INDEX.UNIQUE) ?? []) {
                    unique_fields_new[field] = new Set();
                }

                for (const document of documents) {
                    if (is_document_id_auto) {
                        document._id = 'id' + (document_id_increment++);
                    }
                    else {
                        unique_check_document_ids.push();
                    }

                    for (const field of this._indexes_by_type.get(INDEX.UNIQUE) ?? []) {
                        const value = document[field];

                        if (unique_fields_new[field].has(value)) {
                            throw new Error(`Duplicate value "${value}" for field "${field}".`);
                        }

                        unique_fields_new[field].add(value);

                        unique_check_fields.push(
                            field,
                            value,
                        );
                    }
                }

                // console.log('unique_check_document_ids', unique_check_document_ids);
                // console.log('unique_check_fields', unique_check_fields);

                // must check uniqueness
                if (unique_check_document_ids.length > 0 || unique_check_fields.length > 0) {
                    multi.EVALSHA(
                        this._scripts.CHECK_DUPLICATES,
                        0,
                        insert_op_id,
                        unique_check_document_ids.length,
                        ...unique_check_document_ids,
                        ...unique_check_fields,
                    ).as('duplicates');
                }
                else {
                    multi.SADD(
                        REDIS_KEY_INSERT,
                        insert_op_id,
                    );
                }
            }

            for (const document of documents) {
                const document_id = document._id;
                delete document._id;

                const expire_at = (typeof document._ttl === 'number') ? (unixtime() + document._ttl) : Number.MAX_SAFE_INTEGER;
                delete document._ttl;

                const document_hash_bulk = objectToHashBulk(document);

                multi.EVALSHA(
                    this._scripts.INSERT,
                    0,
                    insert_op_id,
                    document_id,
                    expire_at,
                    document_hash_bulk.length / 2,
                    ...document_hash_bulk,
                    ...this._getIndexBulk(document),
                );
            }

            multi.SREM(
                REDIS_KEY_INSERT,
                insert_op_id,
            );

            const result = await multi.EXEC();
            // console.log('result', result);

            if (hasOwnProperty(result, 'duplicates') && result.duplicates.length > 0) {
                const [ field, value ] = result.duplicates;
                throw new Error(`Duplicate value "${value}" for field "${field}".`);
            }
        }
    }

    async _find (
        {
            filter = {},
            sort = {},
            offset = 0,
            count = null,
        },
        options = null,
    ) {
        await this._loadScripts();

        await this._deleteExpiredDelay();

        const { redisClient } = this;

        const temporaryKeys = new TemporaryKeys(this);

        const multi = redisClient.MULTI();

        const timing = new Timing(multi);

        // const inter_keys = [];
        // const inter_weights = [];
        const inter = [];

        // filter
        for (const [ field, value ] of Object.entries(filter)) {
            switch (this._indexes_by_field.get(field)) {
                case INDEX.UNIQUE: {
                    const values = parseHashedValue(value);
                    if (values.size === 0) {
                        throw new TypeError(`Invalid filter syntax for field "${field}".`);
                    }

                    const tmp_key_local = temporaryKeys.create();

                    multi.EVALSHA(
                        this._scripts.FIND_INDEX_UNIQUE,
                        0,
                        tmp_key_local,
                        field,
                        ...values,
                    );

                    timing.add(`index-unique.${field}`);

                    // inter_keys.push(tmp_key_local);
                    // inter_weights.push(0);
                    inter.push(
                        'S',
                        tmp_key_local,
                    );
                } break;
                case INDEX.HASH: {
                    const values = parseHashedValue(value);

                    switch (values.size) {
                        case 0:
                            throw new TypeError(`Invalid filter syntax for field "${field}".`);
                        case 1:
                            // inter_keys.push(
                            //     this._getRedisKeyIndex(
                            //         field,
                            //         getElement(values),
                            //     ),
                            // );
                            // inter_weights.push(0);
                            inter.push(
                                'S',
                                this._getRedisKeyIndex(
                                    field,
                                    getElement(values),
                                ),
                            );
                        break;
                        default: {
                            const keys = [];
                            for (const value of values) {
                                keys.push(
                                    this._getRedisKeyIndex(field, value),
                                );
                            }

                            inter.push(
                                'SU',
                                keys.length,
                                ...keys,
                            );

                            // const tmp_key_local = temporaryKeys.create();

                            // multi.SUNIONSTORE(
                            //     tmp_key_local,
                            //     keys.length,
                            //     ...keys,
                            // );

                            // timing.add(`index-hash.${field}`);

                            // inter_keys.push(tmp_key_local);
                            // inter_weights.push(0);
                        }
                    }
                } break;
                case INDEX.RANGE: {
                    // let lower = '-inf';
                    // let upper = '+inf';
                    let lower_type = '';
                    let lower_value = 0;
                    let upper_type = '';
                    let upper_value = 0;

                    if (isPlainObject(value)) {
                        if (hasOwnProperty(value, '$gt')) {
                            // lower = '(' + value.$gt;
                            lower_type = 'E';
                            lower_value = value.$gt;
                        }
                        else if (hasOwnProperty(value, '$gte')) {
                            // lower = value.$gte;
                            lower_type = 'I';
                            lower_value = value.$gte;
                        }

                        if (hasOwnProperty(value, '$lt')) {
                            // upper = '(' + value.$lt;
                            upper_type = 'E';
                            upper_value = value.$lt;
                        }
                        else if (hasOwnProperty(value, '$lte')) {
                            // upper = value.$lte;
                            upper_type = 'I';
                            upper_value = value.$lte;
                        }
                    }
                    else if (typeof value === 'number') {
                        // lower = value;
                        // upper = value;
                        lower_type = 'I';
                        lower_value = value;
                        upper_type = 'I';
                        upper_value = value;
                    }
                    else {
                        throw new TypeError(`Invalid filter syntax for field "${field}".`);
                    }

                    // const tmp_key_local = temporaryKeys.create();

                    // multi.ZRANGESTORE(
                    //     tmp_key_local,
                    //     this._getRedisKeyIndex(field),
                    //     lower,
                    //     upper,
                    //     'BYSCORE',
                    // );

                    // timing.add(`index-range.${field}`);

                    // inter_keys.push(
                    //     tmp_key_local,
                    // );
                    // inter_weights.push(0);
                    inter.push(
                        'ZR',
                        this._getRedisKeyIndex(field),
                        lower_type,
                        lower_value,
                        upper_type,
                        upper_value,
                    );
                } break;
                default:
                    throw new Error('Cannot find documents by non-indexed field.');
            }
        }

        let sort_key;
        let sort_direction;
        // sort
        // if (isPlainObject(sort)) {
        //     const sort_entries = Object.entries(sort);
        //     if (sort_entries.length > 0) {
        //         if (sort_entries.length > 1) {
        //             throw new Error('Cannot sort document by more than one field.');
        //         }

        //         let sort_field;
        //         [ sort_field, sort_direction ] = sort_entries[0];

        //         // inter_keys.push(
        //         //     this._getRedisKeyIndex(sort_field),
        //         // );
        //         // inter_weights.push(1);
        //     }
        // }
        if (isPlainObject(sort)) {
            const sort_entries = Object.entries(sort);
            if (sort_entries.length > 0) {
                if (sort_entries.length > 1) {
                    throw new Error('Cannot sort document by more than one field.');
                }

                let sort_field;
                [ sort_field, sort_direction ] = sort_entries[0];

                sort_key = this._getRedisKeyIndex(sort_field);
            }
        }

        const is_result_sorted = typeof sort_key === 'string';

        const tmp_key_result = temporaryKeys.create();
        if (inter.length > 0) {
            multi.EVALSHA(
                this._scripts.FIND_INTER,
                0,
                tmp_key_result,
                sort_key ?? '',
                ...inter,
            );

            timing.add('main-intersection');
        }

        // if (0 === inter_keys.length) {
        //     inter_keys.push(`${this._redis_prefix}:${REDIS_SUBKEY_DOCUMENT_IDS}`);
        //     inter_weights.push(0);
        // }

        // console.log('inter_keys', inter_keys);
        // console.log('inter_weights', inter_weights);

        // const tmp_key_result = temporaryKeys.create();

        // multi.ZINTERSTORE(
        //     tmp_key_result,
        //     inter_keys.length,
        //     ...inter_keys,
        //     'WEIGHTS',
        //     ...inter_weights,
        // );

        // timing.add('main-intersection');

        if (typeof options?.zrange_rank === 'number') {
            multi.ZRANGE(
                tmp_key_result,
                options.zrange_rank,
                options.zrange_rank,
                'WITHSCORES',
            ).as('zrange_element');
        }
        else if (typeof options?.zrange_rank_percent === 'number') {
            multi.EVALSHA(
                this._scripts.FIND_GET_PERCENTILE,
                0,
                tmp_key_result,
                options.zrange_rank_percent,
            ).as('number');

            timing.add('find-percentile');
        }
        else {
            const zrange_args = [];

            if (-1 === sort_direction) {
                zrange_args.push('+inf', '-inf', 'BYSCORE', 'REV');
            }
            else {
                zrange_args.push('-inf', '+inf', 'BYSCORE');
            }

            if (typeof count !== 'number') {
                // counting documents
                if (true === options?.count) {
                    count = 0;
                }
                // deleting documents
                else if (true === options?.delete) {
                    count = 1;
                }
                // getting the result
                else {
                    count = 1_000;
                }
            }

            if (typeof offset === 'number' && typeof count === 'number') {
                count = count >= 0 ? count : -1;
                zrange_args.push(
                    'LIMIT',
                    offset,
                    count,
                );
            }

            // counting documents
            if (true === options?.count) {
                if (is_result_sorted) {
                    multi.ZCARD(tmp_key_result).as('number');
                }
                else {
                    multi.SCARD(tmp_key_result).as('number');
                }

                timing.add('count');
            }
            // deleting documents
            else if (true === options?.delete) {
                multi.EVALSHA(
                    this._scripts.DELETE,
                    0,
                    tmp_key_result,
                    zrange_args.length,
                    ...zrange_args,
                    ...this._getIndexBulk(),
                ).as('number');

                timing.add('delete');
            }
            // getting the result
            else {
                const return_field = options?.return_field ?? '';
                const result_key = (return_field.length > 0) ? 'raw' : 'documents_raw';

                if (is_result_sorted) {
                    console.log('zrange_args', zrange_args);
                    multi.EVALSHA(
                        this._scripts.FIND_GET,
                        0,
                        return_field,
                        'Z',
                        tmp_key_result,
                        ...zrange_args,
                    ).as(result_key);
                }
                else {
                    multi.EVALSHA(
                        this._scripts.FIND_GET,
                        0,
                        return_field,
                        'S',
                        tmp_key_result,
                        count,
                    ).as(result_key);
                }

                timing.add('find-get');
            }
        }

        for (const key of temporaryKeys.keys) {
            multi.PEXPIRE(key, 1);
        }

        timing.add('cleanup');

        const result = await multi.EXEC();
        // console.log('result', result);

        console.log(
            timing.result(result),
        );

        if (result.raw) {
            return result.raw;
        }
        else if (result.documents_raw) {
            const documents_to_return = [];

            for (const document_raw of result.documents_raw) {
                if (document_raw.length > 0) {
                    let document = hashBulkToObject(document_raw);

                    if (this.schema.document_schema) {
                        document = this.schema.document_schema.transform(document);
                    }

                    if (null !== document) {
                        documents_to_return.push(document);
                    }
                }
            }

            return documents_to_return;
        }
        else if (result.zrange_element) {
            return Number.parseFloat(
                result.zrange_element[1],
            );
        }
        else if (result.number) {
            return Number.parseFloat(result.number);
        }
    }

    /* async */ find (find_options) {
        return this._find(find_options);
    }

    /* async */ count (filter) {
        return this._find(
            {
                filter,
            },
            {
                count: true,
            },
        );
    }

    async max (field, filter) {
        const sort = {};
        const options = {};

        if (true === this._indexes_by_type.get(INDEX.RANGE)?.has(field)) {
            sort[field] = 1;
            options.zrange_rank = -1;
        }
        else {
            options.return_field = field;
        }

        const data = await this._find(
            {
                filter,
                sort,
                count: -1,
            },
            options,
        );

        if (Array.isArray(data)) {
            return Math.max(
                ...data.map(value => Number.parseFloat(value)),
            );
        }
        else {
            return data;
        }
    }

    async min (field, filter) {
        const sort = {};
        const options = {};

        if (true === this._indexes_by_type.get(INDEX.RANGE)?.has(field)) {
            sort[field] = 1;
            options.zrange_rank = 0;
        }
        else {
            options.return_field = field;
        }

        const data = await this._find(
            {
                filter,
                sort,
                count: -1,
            },
            options,
        );

        if (Array.isArray(data)) {
            return Math.min(
                ...data.map(value => Number.parseFloat(value)),
            );
        }
        else {
            return data;
        }
    }

    async sum (field, filter) {
        const values = await this._find(
            {
                filter,
                count: -1,
            },
            {
                return_field: field,
            },
        );

        let sum = 0;

        for (const value of values) {
            sum += Number.parseFloat(value);
        }

        return sum;
    }

    async avg (field, filter) {
        const values = await this._find(
            {
                filter,
                count: -1,
            },
            {
                return_field: field,
            },
        );

        let avg = 0;
        let count = 0;

        for (const value of values) {
            avg = ((avg * count) + Number.parseFloat(value)) / ++count;
        }

        return avg;
    }

    /* async */ percentile (field, threshold, filter) {
        const sort = {};
        const options = {};

        if (true === this._indexes_by_type.get(INDEX.RANGE)?.has(field)) {
            sort[field] = 1;
            options.zrange_rank_percent = threshold;
        }
        else {
            throw new Error('Cannot do percentile() over non-indexed field.');
        }

        return this._find(
            {
                filter,
                sort,
                count: -1,
            },
            options,
        );
    }

    /* async */ delete (find_options) {
        return this._find(
            find_options,
            {
                delete: true,
            },
        );
    }

    async _deleteExpired (multi) {
        await this._loadScripts();

        return (multi ?? this.redisClient).EVALSHA(
            this._scripts.DELETE,
            0,
            '',
            0,
            ...this._getIndexBulk(),
        );
    }

    /* async */ _deleteExpiredDelay () {
        if (Date.now() - this._ts_delete_expired > 30_000) {
            this._ts_delete_expired = Date.now();

            return this._deleteExpired();
        }
    }

    // resetIndex (field) {}

    _getRedisKeyIndex (field, value) {
        let key = `${this._redis_prefix}:${REDIS_SUBKEY_INDEX}:${field}`;
        if (this._indexes_by_type.get(INDEX.HASH)?.has(field)) {
            key += `:${value}`;
        }

        return key;
    }

    _getIndexBulk (document) {
        const indexes_bulk = [];

        for (const index_type_id of INDEX._values) {
            for (const field of this._indexes_by_type.get(index_type_id) ?? []) {
                indexes_bulk.push(
                    index_type_id,
                    field,
                );

                if (document) {
                    indexes_bulk.push(
                        document[field],
                    );
                }
            }
        }

        return indexes_bulk;
    }
}

module.exports = RedisStore;
