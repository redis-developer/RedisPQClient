# RedisPQClient &nbsp;[![Build Status](https://travis-ci.com/TusharRakheja/RedisPQClient.svg?branch=master)](https://travis-ci.com/TusharRakheja/RedisPQClient)

RedisPQClient is a [Redis](https://redis.io/)- and DynamoDB-backed priority queue client for Java. It supports standard priority queue operations such as `add`, `poll` and `peek`, and can be used for purposes like job-scheduling, request processing etc.

## Prerequisites
- A Redis instance with the [RedisJSON](https://github.com/RedisJSON/RedisJSON/) module loaded.
- A DynamoDB table with a hash-key of type N called `priority`, and a range-key of type S called `uuid`.

## Usage

```java
import inc.future.redispq.Prioritizable;
import inc.future.redispq.RedisPriorityQueue;

class SamplePrioritizableItem implements Prioritizable {
    // ... implement getPriority(), getUUID(), and getPayload() methods.
}

RedisPriorityQueue redisPriorityQueue = new RedisPriorityQueue(
    APP_NAME, AWS_CREDENTIALS_PROVIDER, REGION, REDIS_HOSTNAME, [PORT], [PASSWORD]
);   // The APP_NAME should be the same as that used for the DynamoDB table.

redisPriorityQueue.add(new SamplePrioritizableItem(100L, "UUID-1", "PAYLOAD-1"));
redisPriorityQueue.add(new SamplePrioritizableItem(100L, "UUID-2", "PAYLOAD-2"));
redisPriorityQueue.add(new SamplePrioritizableItem(1000L, "UUID-3", "PAYLOAD-3"));
redisPriorityQueue.add(new SamplePrioritizableItem(10L, "UUID-4", "PAYLOAD-4"));
redisPriorityQueue.add(new SamplePrioritizableItem(10000L, "UUID-5", "PAYLOAD-5"));
redisPriorityQueue.add(new SamplePrioritizableItem(10000L, "UUID-6", "PAYLOAD-6"));

System.out.println(redisPriorityQueue.peek().toString());  // Will print the item with either UUID-5 or UUID-6, without removing them from the queue.
for (int i = 0; i < 6; i++) {
    System.out.println(redisPriorityQueue.poll().toString());  // Will print items in the order UUID-5/UUID-6, UUID-3, UUID-1/UUID-2, and UUID-4
}
```

## How it works

RedisPQClient client works by keeping track of the highest priority value in Redis via a max-heap, as well as how many items with a given priority it has seen. The full-items, with attributes other than `priority`, such as `uuid` and `payload`, are stored in DynamoDB for durability, with `priority` as the hash-key and `uuid` of the item as range-key. 

To maintain the max-heap in Redis, RedisPQClient uses Lua scripts from the [RedisPQScripts](https://github.com/TusharRakheja/RedisPQScripts) package. The scripts are cached server-side on Redis when the client is initialized, and used to add and remove items from the heap.

### Adding to the queue

For example, consider the workflow below, where a user adds 4 items via a RedisPQClient, initialized with `appName` = "MYAPP". The items are listed on the left side, whereas the state of the Redis and DynamoDB databases after the 4 additions, is on the right.

![Add item flow](https://drive.google.com/uc?id=1Niwhvw3Ocr_bb0Roszc3iJ30vi5RhMnJ)

### Polling from the queue

When the user wants to poll from the queue, the RedisPQClient polls the highest priority value from the Redis max-heap, and queries DynamoDB with the polled value as hash-key. The query only gets a single-item, and that item is returned by RedisPQClient. If multiple items were added with the same priority, there's no guarantee for the order in which those items will be polled. 

For a more in-depth look into the working of RedisPQClient, please watch this YouTube video: [RedisPQClient - RedisConf 2021 Hackathon](https://www.youtube.com/watch?v=iEpVCbWpelQ)

