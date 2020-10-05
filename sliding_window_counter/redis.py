import time
import redis


# redis connection
def get_connection(host="127.0.0.1", port="6379", db=0):
    connection = redis.StrictRedis(host=host, port=port, db=db)
    return connection


class SlidingWindowCounterRateLimiter(object):
    """
    representation of data stored in redis
    metadata
    --------
    "userid_metadata": {
            "requests": 2,
        "window_time": 30
    }

    counts
    -------
    "userid_counts": {
        "bucket1": 2,
        "bucket2": 3
    }
    """
    REQUESTS = "requests"  # key in the metadata representing the max number of requests
    WINDOW_TIME = "window_time"  # key in the metadata representing the window time
    METADATA_SUFFIX = "_metadata"  # metadata suffix
    COUNTS = "_counts"  # count buckets suffix

    def __init__(self, bucketSize=10):
        # bucket size can be coarser than 10 sec based on the window size.
        self.bucketSize = bucketSize  # in seconds
        self.con = get_connection()

    # current timestamp in seconds.
    @classmethod
    def getCurrentTimestampInSec(cls):
        return int(round(time.time()))

    def getBucket(self, timestamp, windowTimeInSec):
        factor = windowTimeInSec / self.bucketSize
        return (timestamp // factor) * factor

    # Adds a new user's rate of requests to be allowed.
    # using redis hashes to store the user metadata.
    def addUser(self, userId, requests=100, windowTimeInSec=60):
        # TODO: Make sure that the given windowTimeInSec is a multiple of bucketSize
        self.con.hmset(userId + self.METADATA_SUFFIX, {
            self.REQUESTS: requests,
            self.WINDOW_TIME: windowTimeInSec
        })

    # Get the user metadata storing the number of requests per window time.
    def getRateForUser(self, userId):
        val = self.con.hgetall(userId + self.METADATA_SUFFIX)
        if val is None:
            raise Exception("Un-registered user: " + userId)
        return int(val[self.REQUESTS]), int(val[self.WINDOW_TIME])

    # Removes a user's metadata and timestamps.
    def removeUser(self, userId):
        self.con.delete(userId + self.METADATA_SUFFIX, userId + self.COUNTS)

    # Atomically increments hash key val by unit and returns. Uses optimistic locking
    # over userId + self.COUNTS redis key.
    def _incrementAHashKeyValByUnitAmotically(self, userId, bucket, redisPipeline):
        # A two element array with first one representing success of updating the
        # bucket value and other giving a list of all the values(counts) of the buckets.
        count = redisPipeline.hmget(userId + self.COUNTS, bucket)[0]
        if count is None:
            count = 0
        currentBucketCount = int(count)
        redisPipeline.multi()
        redisPipeline.hmset(userId + self.COUNTS, {bucket: currentBucketCount + 1})
        redisPipeline.hvals(userId + self.COUNTS)

    # Deciding if the rate has been crossed.
    # we're using redis hashes to store the counts.
    def shouldAllowServiceCall(self, userId):
        allowedRequests, windowTime = self.getRateForUser(userId)
        # evict older entries
        allBuckets = map(int, self.con.hkeys(userId + self.COUNTS))
        currentTimestamp = self.getCurrentTimestampInSec()
        oldestPossibleEntry = currentTimestamp - windowTime
        bucketsToBeDeleted = list(filter(lambda bucket: bucket < oldestPossibleEntry, allBuckets))
        if len(bucketsToBeDeleted) != 0:
            self.con.hdel(userId + self.COUNTS, *bucketsToBeDeleted)
        currentBucket = self.getBucket(currentTimestamp, windowTime)
        # transaction holds an optimistic lock over the redis entries
        # userId + self.METADATA_SUFFIX, userId + self.COUNTS.
        # The changes in _incrementAHashKeyValByUnitAmotically are committed only
        # if none of these entries get changed.
        _, requests = self.con.transaction(
            lambda pipe: self._incrementAHashKeyValByUnitAmotically(userId, currentBucket, pipe),
                userId + self.COUNTS, userId + self.METADATA_SUFFIX
        )
        if sum(map(int, requests)) > allowedRequests:
            return False
        return True