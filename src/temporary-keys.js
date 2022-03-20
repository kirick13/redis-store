
class TemporaryKeys {
    constructor (redisStore) {
        this.keys = new Set();

        this._incr = 0;

        this._redis_prefix = redisStore._redis_prefix;
    }

    create () {
        const key = `${this._redis_prefix}:tmp:${this._incr++}`;
        this.keys.add(key);
        return key;
    }
}

module.exports = TemporaryKeys;
