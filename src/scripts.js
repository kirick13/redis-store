
const { REDIS_SUBKEY_INSERT,
        REDIS_SUBKEY_DOCUMENT_IDS,
        REDIS_SUBKEY_DOCUMENT,
        REDIS_SUBKEY_INDEX       } = require('./consts');

// // arguments:
// // is_expired [ tmp_key_document_ids ] [ field index_type [ field index_type ... ] ]
// // `is_expired` — 1 if you want to delete expired documents, default 0
// // `tmp_key_document_ids` — name of key (sorted set) contains document ids to delete
// exports.DELETE = `
//     --
// `;

module.exports = /* async */ (redisStore) => {
    const redis_prefix = redisStore._redis_prefix;

    // arguments:
    // insert_op_id document_id_count [ document_id ... ] [ field value ... ]
    // every `field` must be unique-indexed
    const CHECK_DUPLICATES = `
        local document_id_count = tonumber(ARGV[2])

        for i = 3, 3 + document_id_count - 1, 1
        do
            local document_id = ARGV[i]

            if redis.call("EXISTS", "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT}:" .. document_id) > 0
            then
                return { "_id", document_id }
            end
        end

        for i = 3 + document_id_count, #ARGV, 2
        do
            local field = ARGV[i]
            local value = ARGV[i + 1]

            if redis.call("HEXISTS", "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. field, value) ~= 0
            then
                return { field, value }
            end
        end

        redis.call("SADD", "${redis_prefix}:${REDIS_SUBKEY_INSERT}", ARGV[1])

        return {}
    `;

    // arguments:
    // insert_op_id document_id expire_at fields_count field value [ field value ... ] [ index_type index_field index_value ... ]
    const INSERT = `
        if redis.call("SISMEMBER", "${redis_prefix}:${REDIS_SUBKEY_INSERT}", ARGV[1]) == 0
        then
            return
        end

        local document_id = ARGV[2]

        redis.call(
            "ZADD",
            "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT_IDS}",
            "NX",
            ARGV[3],
            document_id
        )

        local fields_count = tonumber(ARGV[4])

        redis.call(
            "HSET",
            "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT}:" .. document_id,
            unpack(ARGV, 5, 5 + fields_count * 2 - 1)
        )

        for i = 5 + fields_count * 2, #ARGV, 3
        do
            local index_type = ARGV[i]
            local index_field = ARGV[i + 1]
            local index_value = ARGV[i + 2]

            if "1" == index_type
            then
                redis.call(
                    "HSET",
                    "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. index_field,
                    index_value,
                    document_id
                )
            elseif "2" == index_type
            then
                redis.call(
                    "SADD",
                    "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. index_field .. ":" .. index_value,
                    document_id
                )
            elseif "3" == index_type
            then
                redis.call(
                    "ZADD",
                    "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. index_field,
                    index_value,
                    document_id
                )
            end
        end
    `;

    // arguments:
    // tmp_key index_field index_value [ index_value ... ]
    const FIND_INDEX_UNIQUE = `
        local document_ids = redis.call(
            "HMGET",
            "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. ARGV[2],
            unpack(ARGV, 3)
        )

        redis.call(
            "SADD",
            ARGV[1],
            unpack(document_ids)
        )
    `;

    // arguments:
    // return_field ...zrange_args
    const FIND_GET = `
        local return_field = ARGV[1]
        local result = {}

        local document_ids = redis.call("ZRANGE", unpack(ARGV, 2))

        for _, document_id in pairs(document_ids)
        do
            if '' == return_field
            then
                table.insert(
                    result,
                    redis.call(
                        "HGETALL",
                        "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT}:" .. document_id
                    )
                )
            else
                table.insert(
                    result,
                    redis.call(
                        "HGET",
                        "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT}:" .. document_id,
                        return_field
                    )
                )
            end
        end

        return result
    `;

    // arguments:
    // tmp_key percentile
    const FIND_GET_PERCENTILE = `
        local card = redis.call("ZCARD", ARGV[1])
        local rank = math.max(math.floor(card * tonumber(ARGV[2]) + 0.5), 0)
        return redis.call(
            "ZRANGE",
            ARGV[1],
            rank,
            rank,
            "WITHSCORES"
        )[2]
    `;

    // arguments:
    // tmp_key zrange_args_count [ zrange_arg ... ] [ index_type index_field ... ]
    // if `tmp_key` is empty string, command will delete expired documents
    const DELETE = `
        local tmp_key = ARGV[1]
        local zrange_args_count = tonumber(ARGV[2])

        local document_ids = {}

        if '' == tmp_key
        then
            document_ids = redis.call(
                "ZRANGE",
                "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT_IDS}",
                "-inf",
                redis.call("TIME")[1],
                "BYSCORE"
            )
        else
            document_ids = redis.call(
                "ZRANGE",
                tmp_key,
                unpack(ARGV, 3, 3 + zrange_args_count - 1)
            )
        end

        if #document_ids > 0
        then
            local keys_to_del = {}

            for _, document_id in pairs(document_ids)
            do
                local key = "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT}:" .. document_id

                table.insert(keys_to_del, key)

                for i = 3 + zrange_args_count, #ARGV, 2
                do
                    local index_type = ARGV[i]
                    local index_field = ARGV[i + 1]

                    if "1" == index_type
                    then
                        redis.call(
                            "HDEL",
                            "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. index_field,
                            redis.call("HGET", key, index_field)
                        )
                    elseif "2" == index_type
                    then
                        redis.call(
                            "SREM",
                            "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. index_field .. ":" .. redis.call("HGET", key, index_field),
                            document_id
                        )
                    elseif "3" == index_type
                    then
                        redis.call(
                            "ZREM",
                            "${redis_prefix}:${REDIS_SUBKEY_INDEX}:" .. index_field,
                            document_id
                        )
                    end
                end
            end

            redis.call(
                "ZREM",
                "${redis_prefix}:${REDIS_SUBKEY_DOCUMENT_IDS}",
                unpack(document_ids)
            )

            if #keys_to_del > 0
            then
                redis.call(
                    "DEL",
                    unpack(keys_to_del)
                )
            end
        end

        return #document_ids
    `;

    return redisStore.redisClient.MULTI()
    .SCRIPT('LOAD', CHECK_DUPLICATES).as('CHECK_DUPLICATES')
    .SCRIPT('LOAD', INSERT).as('INSERT')
    .SCRIPT('LOAD', FIND_INDEX_UNIQUE).as('FIND_INDEX_UNIQUE')
    .SCRIPT('LOAD', FIND_GET).as('FIND_GET')
    .SCRIPT('LOAD', FIND_GET_PERCENTILE).as('FIND_GET_PERCENTILE')
    .SCRIPT('LOAD', DELETE).as('DELETE')
    .EXEC();
};
