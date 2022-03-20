
const hasOwnProperty = (target, key) => Object.prototype.hasOwnProperty.call(target, key);
const isPlainObject = (value) => typeof value === 'object' && null !== value && value.constructor.name === 'Object';
const unixtime = () => Math.floor(Date.now() / 1e3);
const getID = () => String(Date.now()) + Math.random();
const getElement = (iterable) => {
    // eslint-disable-next-line no-unreachable-loop
    for (const value of iterable) {
        return value;
    }
};

const parseHashedValue = (value) => {
    let values = new Set();
    if (isPlainObject(value)) {
        if (hasOwnProperty(value, '$in')) {
            values = new Set(value.$in);
        }
    }
    else {
        values.add(value);
    }

    return values;
};

module.exports = {
    hasOwnProperty,
    isPlainObject,
    unixtime,
    getID,
    getElement,
    parseHashedValue,
};
