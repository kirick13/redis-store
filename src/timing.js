
const PREFIX_NAME = 't.';
const NAME_INIT = 'init';

const parseRedisTime = (time) => (Number.parseFloat(time[0]) * 1_000) + (Number.parseFloat(time[1]) / 1_000);
const normalizeFloat = (value) => Number.parseFloat(value.toFixed(2));

class Timing {
    constructor (multi) {
        this.multi = multi;
        this.names = [];

        this.add(NAME_INIT);
    }

    add (name) {
        this.names.push(name);
        this.multi.TIME().as(PREFIX_NAME + name);
    }

    result (result) {
        const timings = [];

        const ts_from = parseRedisTime(result[PREFIX_NAME + NAME_INIT]);
        let ts_prev = ts_from;

        for (const name of this.names) {
            if (name !== NAME_INIT) {
                const ts = parseRedisTime(result[PREFIX_NAME + name]);
                timings.push(`${name}: ${normalizeFloat(ts - ts_prev)} ms (${normalizeFloat(ts - ts_from)} ms)`);

                ts_prev = ts;
            }
        }

        return timings.join('\n');
    }
}

module.exports = Timing;
