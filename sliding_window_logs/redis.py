import time
import redis


# redis connection
def get_connection(host="127.0.0.1", port="6379", db=0):
    connection = redis.StrictRedis(host=host, port=port, db=db)
    return connection


class SlidingWindowLogRatelimiter(object):
    """
    representation of data stored in redis
    metadata
    --------
    "userid_metadata": {
            "requests": 2,
        "window_time": 30
    }

    timestamps
    ----------
    "userid_timestamps": sorted_set([
        "ts1": "ts1",
        "ts2": "ts2"
    ])
    """
    REQUESTS = "requests"
    WINDOW_TIME = "window_time"
    METADATA_SUFFIX = "_metadata"
    TIMESTAMPS = "_timestamps"
    INF = 9999999999

    def __init__(self):
        self.con = get_connection()

    # timestamp in seconds
    @classmethod
    def getCurrentTimestampInSec(cls):
        return int(round(time.time()))

    # Adds a new user's rate of requests to be allowed
    def addUser(self, userId, requests=100, windowTimeInSec=60):
        self.con.hmset(userId + self.METADATA_SUFFIX, {
            self.REQUESTS: requests,
            self.WINDOW_TIME: windowTimeInSec
        })

    # get the user metadata storing the number of requests per window time
    def getRateForUser(self, userId):
        val = self.con.hgetall(userId + self.METADATA_SUFFIX)
        if val is None:
            raise Exception("Un-registered user: " + userId)
        return int(val[self.REQUESTS]), int(val[self.WINDOW_TIME])

    # Removes a user's metadata and timestamps
    def removeUser(self, userId):
        self.con.delete(userId + self.METADATA_SUFFIX, userId + self.TIMESTAMPS)

    # Atomically add an element to the timestamps and return the total number of requests
    # in the current window time.
    def addTimeStampAtomicallyAndReturnSize(self, userId, timestamp):
        # Transaction holds an optimistic lock over the redis entries userId + self.METADATA_SUFFIX
        # and userId + self.TIMESTAMPS. The changes in _addNewTimestampAndReturnTotalCount
        # are committed only if none of these entries get changed through out
        _, size = self.con.transaction(
            lambda pipe: self._addNewTimestampAndReturnTotalCount(userId, timestamp, pipe),
            userId + self.METADATA_SUFFIX, userId + self.TIMESTAMPS
        )
        return size

    def _addNewTimestampAndReturnTotalCount(self, userId, timestamp, redisPipeline):
        # A two element array with first one representing success of adding an element into
        # sorted set and other as the count of the sorted set is returned by this method
        redisPipeline.multi()
        redisPipeline.zadd(userId + self.TIMESTAMPS, timestamp, timestamp)
        redisPipeline.zcount(userId + self.TIMESTAMPS, 0, self.INF)

    # decide to allow a service call or not
    # we use sorted sets datastructure in redis for storing our timestamps.
    # For more info, visit https://redis.io/topics/data-types
    def shouldAllowServiceCall(self, userId):
        maxRequests, unitTime = self.getRateForUser(userId)
        currentTimestamp = self.getCurrentTimestampInSec()
        # evict older entries
        oldestPossibleEntry = currentTimestamp - unitTime
        # removes all the keys from start to oldest bucket
        self.con.zremrangebyscore(userId + self.TIMESTAMPS, 0, oldestPossibleEntry)
        currentRequestCount = self.addTimeStampAtomicallyAndReturnSize(
            userId, currentTimestamp
        )
        print(currentRequestCount, maxRequests)
        if currentRequestCount > maxRequests:
            return False
        return True
