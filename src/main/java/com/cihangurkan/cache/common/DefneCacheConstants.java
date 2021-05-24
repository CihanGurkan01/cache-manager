package com.cihangurkan.cache.common;

import org.redisson.api.RMapCache;

public class DefneCacheConstants {

    public DefneCacheConstants() {}

    /**
     * Name of the map holding requests.
     */
    public static final String REQUEST_MAP = "REQUEST_MAP_";

    /**
     * Name of the map holding reference value to handle request timeouts. When a Runnable task is
     * scheduled for timeout and response is received before timeout elapses entry from this map
     * will be removed. So when the entry is removed from this map, we will ignore the Runnable task
     * when it is triggered.
     */
    public static final String REQUEST_TIMEOUT_MAP = "REQUEST_TIMEOUT_MAP";

    /**
     * Name prefix of the expirable (items with time-to-live period) objects. Redis currently does
     * not support item expiration for queues, so expirable queue implementation is managed by a
     * map for Redis ({@link RMapCache}).
     */
    public static final String EXPIRABLE_OBJECT_NAME_PREFIX = "EXPIRABLE_";
}
