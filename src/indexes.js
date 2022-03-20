
const INDEX = {
    UNIQUE: 1,
    HASH  : 2,
    RANGE : 3,
};
INDEX._values = new Set(Object.values(INDEX));

module.exports = INDEX;
