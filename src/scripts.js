
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
    // tmp_key sort_key [ type [ keys_count ] key [ key ... ] [ lower_type lower_value upper_type upper_value ] ... ]
    // `type` is a definition of key type and operation type:
    // -  S: only one key holding a set
    // - SU: many keys holding sets with union operation between them
    // -  Z: only one key holding a sorted set
    // - ZR: only one key holging a sorted set, but in range of lower and upper (inclusive)
    const FIND_INTER = `
        local tmp_key = ARGV[1]

        local src_primary = nil
        local src_secondary = {}
        local card_min = nil
        local i = 3
        while i < #ARGV
        do
            local incr = 1
            local type = ARGV[i]
            local el = { type = type }
            local card = 0

            if "S" == type
            then
                el["key"] = ARGV[i + 1]
                incr = 2
                card = redis.call("SCARD", el["key"])
            elseif "SU" == type
            then
                local keys_count = ARGV[i + 1]
                el["keys"] = { unpack(ARGV, i + 2, i + 2 + keys_count - 1) }
                -- redis.log(redis.LOG_NOTICE, "SU keys = " .. cjson.encode(el["keys"]))
                incr = 2 + keys_count
                for _, key in ipairs(el["keys"])
                do
                    card = card + redis.call("SCARD", key)
                end
            elseif "Z" == type
            then
                el["key"] = ARGV[i + 1]
                incr = 2
                card = redis.call("ZCARD", el["key"])
            elseif "ZR" == type
            then
                el["key"] = ARGV[i + 1]
                el["lower_type"] = ARGV[i + 2]
                el["lower_value"] = tonumber(ARGV[i + 3])
                el["upper_type"] = ARGV[i + 4]
                el["upper_value"] = tonumber(ARGV[i + 5])
                incr = 6

                el["lower"] = "-inf"
                el["upper"] = "+inf"
                if "I" == el["lower_type"]
                then
                    el["lower"] = "" .. el["lower_value"]
                elseif "E" == el["lower_type"]
                then
                    el["lower"] = "(" .. el["lower_value"]
                end
                if "I" == el["upper_type"]
                then
                    el["upper"] = "" .. el["upper_value"]
                elseif "E" == el["upper_type"]
                then
                    el["upper"] = "(" .. el["upper_value"]
                end
                card = redis.call("ZCOUNT", el["key"], el["lower"], el["upper"])
            else
                return redis.error_reply("Invalid type given.")
            end

            -- redis.log(redis.LOG_NOTICE, "card = " .. card .. " for key = " .. (el["key"] or cjson.encode(el["keys"])))

            if nil == card_min or card < card_min
            then
                table.insert(src_secondary, src_primary)
                src_primary = el
                card_min = card
            else
                table.insert(src_secondary, el)
            end
            i = i + incr
        end

        -- redis.log(redis.LOG_NOTICE, "card_min = " .. card_min)
        -- redis.log(redis.LOG_NOTICE, "src_primary = " .. cjson.encode(src_primary))
        -- redis.log(redis.LOG_NOTICE, "src_secondary = " .. cjson.encode(src_secondary))

        local document_ids = {}
        if "S" == src_primary["type"]
        then
            document_ids = redis.call("SMEMBERS", src_primary["key"])
        elseif "SU" == src_primary["type"]
        then
            document_ids = redis.call("SUNION", unpack(src_primary["keys"]))
        elseif "Z" == src_primary["type"]
        then
            document_ids = redis.call("ZRANGE", src_primary["key"], 0, -1)
        elseif "ZR" == src_primary["type"]
        then
            document_ids = redis.call("ZRANGE", src_primary["key"], src_primary["lower"], src_primary["upper"], "BYSCORE")
        end

        -- redis.log(redis.LOG_NOTICE, "document_count = " .. #document_ids)

        for _, document_id in pairs(document_ids)
        do
            local exists = 1

            for _, el in pairs(src_secondary)
            do
                if "S" == el["type"]
                then
                    exists = redis.call("SISMEMBER", el["key"], document_id)
                elseif "SU" == el["type"]
                then
                    exists = 0
                    for _, key in pairs(el["keys"])
                    do
                        if redis.call("SISMEMBER", key, document_id) == 1 then
                            exists = 1
                            break
                        end
                    end
                elseif "Z" == el["type"]
                then
                    exists = nil ~= redis.call("ZSCORE", el["key"], document_id)
                elseif "ZR" == el["type"]
                then
                    local score = tonumber(redis.call("ZSCORE", el["key"], document_id))

                    if "I" == el["lower_type"]
                    then
                        if score >= el["lower_value"]
                        then else
                            exists = 0
                            break
                        end
                    elseif "E" == el["lower_type"]
                    then
                        if score > el["lower_value"]
                        then else
                            exists = 0
                            break
                        end
                    end

                    if "I" == el["upper_type"]
                    then
                        if score <= el["upper_value"]
                        then else
                            exists = 0
                            break
                        end
                    elseif "E" == el["upper_type"]
                    then
                        if score < el["upper_value"]
                        then else
                            exists = 0
                            break
                        end
                    end
                end

                if 1 ~= exists
                then
                    break
                end
            end

            if 1 == exists
            then
                if "" == ARGV[2]
                then
                    redis.call("SADD", tmp_key, document_id)
                else
                    redis.call("ZADD", tmp_key, redis.call("ZSCORE", ARGV[2], document_id), document_id)
                end
            end
        end

        -- redis.log(redis.LOG_NOTICE, "document_count = " .. redis.call("SCARD", tmp_key))
    `;

    // arguments:
    // return_field type tmp_key [ members_count ] [ zrange_arg ... ]
    const FIND_GET = `
        local document_ids = nil
        if "S" == ARGV[2]
        then
            if "-1" == ARGV[4]
            then
                document_ids = redis.call("SMEMBERS", ARGV[3])
            else
                document_ids = redis.call("SRANDMEMBER", ARGV[3], ARGV[4])
            end
        elseif "Z" == ARGV[2]
        then
            document_ids = redis.call("ZRANGE", ARGV[3], unpack(ARGV, 4))
        end

        local result = {}
        for _, document_id in pairs(document_ids)
        do
            if "" == ARGV[1]
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
                        ARGV[1]
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
    .SCRIPT('LOAD', FIND_INTER).as('FIND_INTER')
    .SCRIPT('LOAD', FIND_GET).as('FIND_GET')
    .SCRIPT('LOAD', FIND_GET_PERCENTILE).as('FIND_GET_PERCENTILE')
    .SCRIPT('LOAD', DELETE).as('DELETE')
    .EXEC();
};
