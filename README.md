
# redis-store

Store you data in Redis and get it using indexes.

That package designed to help you to build a database using Redis' commands and some simple Lua scripts. You can store documents similar to MongoDB, you can find them, order and perform simple aggregations. It can be used, for example, to store and query some logs.

Redis server will know nothing about your database: schema of your database is only described at redis client.

All operations are designed to be atomic, so RedisStore guarantees that no operation will intercept another operation.

## Installation

Run `npm install @kirick/redis-client @kirick/redis-store oh-my-props` to install RedisClient, RedisStore and OhMyProps.

## What can you do?

```javascript
// First of all, import packages
import { createClient } from '@kirick/redis-client';
import RedisStore       from '@kirick/redis-store';
import OhMyProps        from 'oh-my-props';

// create connection to Redis
const redisClient = createClient({ url: 'redis://localhost' });

// and describe your database
// imagine a database of some users
const redisStore = new RedisStore(
    redisClient,
    {
        // name of your database
        namespace: 'users',
        // list of indexes
        index: [
            {
                field: 'user_id',
                type: RedisStore.INDEX.UNIQUE, // unique index: you will not able to insert more than one document with the same value
            },
            {
                field: 'user_name',
                type: RedisStore.INDEX.HASH, // hash index: you will able to find documents with specific value
            },
            {
                field: 'balance',
                type: RedisStore.INDEX.RANGE, // range index: you will able to find documents between specific values
            },
            // you can documents with any other properties you want
            // but you will not able to search by that fields
        ],
        // remember that all properties will be converted to strings by Redis
        // so you can use OhMyProps to convert documents coming from Redis
        document_schema: new OhMyProps({
            user_id: {
                type: Number,
                type_cast: true,
                validator: (value) => Number.isInteger(value) && value >= 0,
            },
            user_name: {
                type: String,
            },
            ts_created: {
                type: Number,
                type_cast: true,
                validator: (value) => Number.isInteger(value) && value >= 0,
            },
        }),
    },
);
```

### Adding new documents

```javascript
// you can insert one document
await redisStore.insertOne({
    user_id  : 1,
    user_name: 'heavydog',
    balance  : 10,
});

// or you can insert array of documents at once
await redisStore.insertMany([
    {
        user_id  : 2,
        user_name: 'redelephant',
        balance  : 23,
    },
    {
        user_id  : 3,
        user_name: 'sadmeercat',
        balance  : 53,
    },
    {
        user_id  : 4,
        user_name: 'happycat',
        balance  : 12,
    },
]);
```

You can add special property `_ttl` to document to limit lifetime of that document in seconds. By now, RedisStore **does not guarantee** that you will never get expired documents from RedisStore in order to achieve high performance.

### Finding documents

You can find documents using this method. It accepts an object with properties `filter`, `order`, `offset` and `count`.

#### Using `filter`

Describe properties' values to find documents. Multiple properties will be combined using AND logical operation.

```javascript
//
await redisStore.find({
    filter: {
        user_id: 1,
    },
});
// -> [{ user_id: 1, user_name: 'heavydog', balance: 10 }]

await redisStore.find({
    filter: {
        user_id: 1,
        balance: 123,
    },
});
// -> []
```

You can use `$in` to query by many values at once. `$in` is available only on `UNIQUE` and `HASH` indexes.

```javascript
await redisStore.find({
    user_id: {
        $in: [ 1, 4, 10 ],
    },
});
// -> [{ user_id: 1, user_name: 'heavydog', balance: 10 },
//     { user_id: 4, user_name: 'happycat', balance: 12 }]
```

To find documents by `RANGE` index, use `$gt`, `$gte`, `$lt` and `$lte`.

```javascript
// let's find users with balances from 12 (inclusive) to 53 (exclusive)
await redisStore.find({
    balance: {
        $gte: 12,
        $lt : 53,
    },
});
// -> [{ user_id: 3, user_name: 'sadmeercat', balance: 53 },
//     { user_id: 4, user_name: 'happycat'  , balance: 12 }]
```

By now you **can not** build complex filters using `and` / `or` / `not` or other logical operations.

By now you **can not** perform filtering by non-indexed fields.

#### Using `order`

It's easy to order documents by field with `RANGE` index:

```javascript
// let's find all document ordered by date of registration from newest ones:
await redisStore.find({
    order: {
        balance: -1, // set 1 to order ascending, -1 for descending
    },
});
// -> [{ user_id: 3, user_name: 'sadmeercat' , balance: 53 },
//     { user_id: 2, user_name: 'redelephant', balance: 23 },
//     { user_id: 4, user_name: 'happycat'   , balance: 12 },
//     { user_id: 1, user_name: 'heavydog'   , balance: 10 }]
```

By now you **can not** order document by more than **one field**.

#### Using `offset` and `count`

You can set **offset** and **count** to get only few documents:

```javascript
// let's find all document but ordered by date of registration from newest ones:
await redisStore.find({
    order: {
        balance: -1,
    },
    offset: 1, // skip 1 document
    count : 2, // get 2 documents
});
// -> [{ user_id: 2, user_name: 'redelephant', balance: 23 },
//     { user_id: 4, user_name: 'happycat'   , balance: 12 }]
```

Note: by default, `count` is equal to 1000. Set it to `-1` to get all matching documents.

### Counting documents

Use `.count()` method to get count of matching documents. This method accepts `filter` object itself.

```javascript
// let's count all documents:
await redisStore.count();
// -> 4

// let's find count of users with balance greater than 15:
await redisStore.count({
    balance: {
        $gt: 15,
    },
});
// -> 2
```

### Deleting documents

To delete documents you should find them using properties of `.find()` method.

```javascript
await redisStore.delete({
    filter: {
        user_id: {
            $in: [ 1, 4, 10 ],
        },
    },
});
```

Note: by default, `count` is equal to 1. Set it to `-1` to delete all matching documents.

### Aggregation

RedisStore offers simple aggregation functions.

#### Maximum

```javascript
await redisStore.max(
    // field name
    'balance',
    // `filter` object similar to `.find()` method
    {}, // finding all documents
);
// -> 53
```

Note: you can call that method over field that doesn't have `RANGE` index.

#### Minimum

```javascript
await redisStore.min(
    // field name
    'balance',
    // `filter` object similar to `.find()` method
    {}, // finding all documents
);
// -> 10
```

Note: you can call that method over field that doesn't have `RANGE` index.

#### Average

```javascript
await redisStore.avg(
    // field name
    'balance',
    // `filter` object similar to `.find()` method
    {}, // finding all documents
);
// -> 24.5
```

Note: that method does not work with indexes.

#### Percentile

```javascript
await redisStore.percentile(
    // field name
    'balance',
    // threshold (value between 0 and 1)
    0.25, // will find 25-percentile
    // `filter` object similar to `.find()` method
    {}, // finding all documents
);
// -> 12
```

Note: you can **not** call that method over field that doesn't have `RANGE` index.
