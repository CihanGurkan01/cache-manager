package com.cihangurkan.cache.util;

/* 
*******************************************************************************
*                                                                             *
*                                                                             *
* Copyright (c) Defne A.S.                                                    *
* All rights reserved.                                                        *
*                                                                             *
* This software is confidential and proprietary information of Defne.         *
* You shall not disclose, reproduce and use in whole or in part.              * 
* You shall use it only in accordance with aggrement or permission of Defne.  *
*                                                                             *   
******************************************************************************/
/* $Id: CacheUtil.java Jul 10, 2019 2:48:37 PM Okan ARDIC $ */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RedissonClient;
import org.redisson.api.annotation.RInject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cihangurkan.cache.abstracts.CacheManager;
import com.cihangurkan.cache.abstracts.EntryExpiredListener;
import com.cihangurkan.cache.common.DefneCacheConstants;
import com.cihangurkan.cache.concrete.CacheEventListener;
import com.cihangurkan.cache.concrete.DistItemConfig;
import com.cihangurkan.cache.concrete.EntryEvent;

/**
 * This class contains utility methods for distributed cache operations.
 * 
 * @author Okan ARDIC
 *
 */
public class CacheUtil {
    private static final Logger logger = LoggerFactory.getLogger(CacheUtil.class);

    private static CacheManager cacheManager;

    private static CacheEventListener cacheEventListener;

    private CacheUtil() {}

    public static void init(CacheManager cacheManager, CacheEventListener callEventListener) {
        CacheUtil.cacheManager = cacheManager;
        CacheUtil.cacheEventListener = callEventListener;

        // Register remote service for RMI
        cacheManager.registerRemoteService(EntryExpiredListener.class, callEventListener);

        /* The issue with EntryExpiredListener (for Redisson) is that, notifications might come
         * after some delay, so that there's no guarantee to receive immediate notification for the
         * entry expiration. Another option is using CacheManager.scheduleTask() method. */
        //        // Init distributed map to store requests with a ttl value and expiry listener.
        //        CacheUtil.initMap(ApplicationConstants.REQUEST_MAP, Integer.MAX_VALUE,
        //                          callCenterManager.getSettings().getMakeCallRequestTimeout(),
        //                          callEventListener);
    }

    /**
     * Stores the given request with its reference number in distributed cache. Given entry will not
     * expire.
     * 
     * @param ref reference number
     * @param request request instance to be stored in cache
     */
    public static <T extends Serializable> void storeRequestByRef(String ref, T request) {
        storeRequestByRef(ref, request, 0, TimeUnit.SECONDS);
    }

    /**
     * Stores the given request with its reference number in distributed cache for the given time
     * period. Entries which were not removed from the cache within the given time period will be
     * automatically evicted from the cache and {@link EntryEvent} will be triggered for each entry
     * evicted.
     * 
     * @param ref reference number
     * @param request request instance to be stored in cache
     * @param ttl time-to-live period to keep the entries inside the cache
     * @param timeUnit time granularity to be used with ttl parameter
     * @see CallEventListener#entryExpired(com.defne.ccm.dist.EntryEvent)
     */
    public static <T extends Serializable> void storeRequestByRef(String ref,
                                                                  T request,
                                                                  long ttl,
                                                                  TimeUnit timeUnit) {
        Serializable value = request;

        // ttl cannot be a negative value
        if (ttl < 0)
            ttl = 0;

        /* Since we trigger an event for entry expiration, items will be removed manually inside the
         * CallEventListener.entryExpired method. Anyway in case of any failures, we still use a ttl
         * value for each entry added to cache with a doubled ttl value to ensure stale entry
         * eviction.
         * TODO: is this necessary if ttl > 0? */
        cacheManager.addToMap(DefneCacheConstants.REQUEST_MAP, ref, value, ttl * 2, timeUnit);

        // ttl = 0 means no expiration
        if (ttl > 0) {
            /* Store Reference Value -> Boolean (true) pairs to control if the timeout task should
             * be executed or not. When a Runnable task is scheduled for timeout and response is
             * received before timeout elapses entry from this map will be removed. So when the
             * entry is removed from this map, we will ignore the Runnable task when it is
             * triggered. */
            cacheManager.addToMap(DefneCacheConstants.REQUEST_TIMEOUT_MAP, ref, true, ttl * 2,
                                  timeUnit);

            // Set scheduled task to notify entry expiration
            cacheManager.scheduleTask(new DistributedRunnable(cacheEventListener, ref, value), ttl,
                                      timeUnit);
        }
    }

    /**
     * Returns true if the request map contains a mapping for the specified reference value.
     * 
     * @param ref reference value of the request
     * @return true if the request map contains a mapping for the specified reference value
     */
    public static boolean containsRequestByRef(String ref) {
        return cacheManager.getMap(DefneCacheConstants.REQUEST_MAP).containsKey(ref);
    }

    /**
     * Retrieves the request object from the distributed cache using the given reference number.
     * 
     * @param ref reference number of the request
     * @return the object associated to the given key or null if no object found with the given key
     */
    public static <T extends Serializable> T getRequestByRef(String ref) {
        return (T) cacheManager.getMap(DefneCacheConstants.REQUEST_MAP).get(ref);
    }

    /**
     * Removes the request object from the distributed cache using the given reference number and
     * returns the removed object if found.
     * 
     * @param ref reference number of the request
     * @return the removed object if found, otherwise null
     */
    public static <T extends Serializable> T removeRequestFromCache(String ref) {
        return (T) cacheManager.removeFromMap(DefneCacheConstants.REQUEST_MAP, ref);
    }

    /**
     * Initializes distributed map with a time-to-live value, max size and entry
     * eviction listener to be triggered for the items whose time-to-live duration expire.
     * 
     * @param name name of the map to set the configuration
     * @param maxSize maximum size of the map
     * @param ttlSeconds default time-to-live in seconds for each entry put to map. This value can
     *            be overridden while adding items to map. (
     *            {@code CacheManager.addToMap(String, Object, Object, long, TimeUnit)}).
     *            ttlSeconds <= 0 means no timeout.
     * @param listener listener implementation to notify entry expiration
     * @see {@link EntryExpiredListener}
     */
    public static void initMap(String name,
                               int maxSize,
                               int ttlSeconds,
                               EntryExpiredListener<Object, Object> listener) {
        DistItemConfig<Object, Object> config = new DistItemConfig<>();
        config.setMaxSize(maxSize);
        config.setTimeToLiveSeconds(ttlSeconds);
        config.setEntryExpiredListener(listener);
        cacheManager.setMapConfig(name, config);
    }

    /**
     * Initializes distributed queue with a time-to-live value, max size and entry eviction listener
     * to be triggered for the items whose time-to-live duration expire.
     * 
     * @param name
     * @param listener
     * @see {@link EntryExpiredListener}
     */
    public static <K, V> void initQueue(String name,
                                        int maxSize,
                                        int ttlSeconds,
                                        EntryExpiredListener<K, V> listener) {
        DistItemConfig<K, V> config = new DistItemConfig<>();
        config.setMaxSize(maxSize);
        config.setTimeToLiveSeconds(ttlSeconds);
        config.setEntryExpiredListener(listener);
        cacheManager.setQueueConfig(name, config);
    }

    /**
     * Poll request from queue by given queue name.
     * 
     * @param queueName Queue name
     * @return
     */
    public static <T extends Serializable> T pollRequestFromPriorityQueueByQueueName(String queueName) {
        Object item = cacheManager.pollFromPriorityQueue(queueName);
        cacheManager.removeFromPriorityQueue(queueName, item);
        return (T) item;
    }

    /**
     * Poll requests from queue by given queue name and poll size.
     * 
     * @param queueName Queue name
     * @param poolSize Poll size
     * @return
     */
    public static <T extends Serializable> List<T> pollRequestFromPriorityQueueByQueueName(String queueName,
                                                                                           int poolSize) {
        List<T> polledRequests = new ArrayList<T>();
        for (int i = 0; i < poolSize; i++) {
            Object item = cacheManager.pollFromPriorityQueue(queueName);
            if (Objects.nonNull(item)) {
                polledRequests.add((T) item);
                cacheManager.removeFromPriorityQueue(queueName, item);
            } else
                break;
        }
        return polledRequests;
    }

    /**
     * Add request to queue by given queue name.
     * 
     * @param queueName Queue name
     * @param item Item sent to queue.
     */
    public static <T extends Serializable> void addRequestToPriorityQueueByQueueName(String queueName,
                                                                                     Comparator<T> comparator,
                                                                                     T item) {
        cacheManager.addItemToPriorityQueue(queueName, item, comparator);
    }

    /**
     * Add request to queue by given queue name.
     * 
     * @param queueName Queue name
     * @param item Items sent to queue.
     */
    public static <T extends Serializable> void addRequestToPriorityQueueByQueueName(String queueName,
                                                                                     Comparator<T> comparator,
                                                                                     List<T> items) {
        for (T item : items)
            cacheManager.addItemToPriorityQueue(queueName, item, comparator);
    }

    /**
     * {@link Runnable} implementation to be used for distributed task scheduling by Hazelcast and
     * Redisson.
     * 
     * @author Okan ARDIC
     *
     */
    private static class DistributedRunnable implements Runnable, Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * This field is injected by Redisson when the task is triggered. It has no effect for
         * Hazelcast.
         */
        @RInject
        private transient RedissonClient redissonClient;

        @SuppressWarnings("rawtypes")
        private transient EntryExpiredListener expiryListener;
        private Serializable key;
        private Serializable value;

        @SuppressWarnings("rawtypes")
        public DistributedRunnable(EntryExpiredListener expiryListener,
                                   Serializable key,
                                   Serializable value) {
            this.expiryListener = expiryListener;
            this.key = key;
            this.value = value;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public void run() {
            boolean removed = false;

            if (redissonClient == null) {
                removed = cacheManager.removeFromMap(DefneCacheConstants.REQUEST_TIMEOUT_MAP,
                                                     key) != null;
            } else {
                removed = redissonClient.getMapCache(DefneCacheConstants.REQUEST_TIMEOUT_MAP)
                                .remove(key) != null;
            }
            removed = true;
            /* If key is still inside the map, then trigger the event. Otherwise it means that this
             * entry was already deleted (handled) by another task. */
            if (removed) {
                /* Hazelcast can use listener class or other class references when received via
                 * constructors and methods, but Redisson cannot do that. So we use @RInject
                 * annotation to inject RedissonClient instance and retrieve EntryExpiredListener
                 * via Redisson. */
                EntryExpiredListener remoteListener = redissonClient == null ? expiryListener
                                                                             : redissonClient.getRemoteService()
                                                                                             .get(EntryExpiredListener.class);

                remoteListener.entryExpired(new EntryEvent<Object, Object>(key, value));
            }
        }
    }
}
