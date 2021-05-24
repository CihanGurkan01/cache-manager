/* 
*******************************************************************************
*                                                                             *
*                                                                             *
* Copyright (c) cihangurkan A.S.                                                    *
* All rights reserved.                                                        *
*                                                                             *
* This software is confidential and proprietary information of cihangurkan.         *
* You shall not disclose, reproduce and use in whole or in part. 			  *	
* You shall use it only in accordance with aggrement or permission of cihangurkan.  *
*                                                                             *   
******************************************************************************/
/* $Id: HazelcastCacheManager.java Feb 25, 2019 10:22:48 AM Okan ARDIC $ */
package com.cihangurkan.cache.hazelcast.concrete;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.cihangurkan.cache.abstracts.CacheManager;
import com.cihangurkan.cache.common.DefneCacheConstants;
import com.cihangurkan.cache.concrete.DistItemConfig;
import com.cihangurkan.cache.concrete.EntryEvent;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

/**
 * Hazelcast cache implementation of {@link CacheManager} interface.
 * 
 * @author Okan ARDIC
 *
 */
public class HazelcastCacheManager extends CacheManager {

    private static Logger logger = LoggerFactory.getLogger(HazelcastCacheManager.class);

    private ApplicationContext applicationContext;

    public HazelcastCacheManager(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    private HazelcastInstance hazelcastInstance;

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#lock(java.lang.String) */
    @Override
    public void lock(String key) {
        hazelcastInstance.getLock(key).lock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#lock(java.lang.String, int) */
    @Override
    public void lock(String key, int leaseTime) {
        lock(key, TimeUnit.SECONDS, leaseTime);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#lock(java.lang.String, java.util.concurrent.TimeUnit,
     * int) */
    @Override
    public void lock(String key, TimeUnit timeUnit, int leaseTime) {
        hazelcastInstance.getLock(key).lock(leaseTime, timeUnit);

    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#tryLock(java.lang.String) */
    @Override
    public boolean tryLock(String key) {
        return hazelcastInstance.getLock(key).tryLock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#tryLock(java.lang.String, int, int) */
    @Override
    public boolean tryLock(String key, int waitTime, int leaseTime) {
        return tryLock(key, TimeUnit.SECONDS, waitTime, leaseTime);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#tryLock(java.lang.String, java.util.concurrent.TimeUnit,
     * int, int) */
    @Override
    public boolean tryLock(String key, TimeUnit timeUnit, int waitTime, int leaseTime) {
        try {
            return hazelcastInstance.getLock(key).tryLock(waitTime, TimeUnit.SECONDS, leaseTime,
                                                          TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#unlock(java.lang.String) */
    @Override
    public void unlock(String key) {
        ILock lock = hazelcastInstance.getLock(key);
        if (lock.isLockedByCurrentThread())
            lock.unlock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#forceUnlock(java.lang.String) */
    @Override
    public void forceUnlock(String key) {
        hazelcastInstance.getLock(key).forceUnlock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#getMap(java.lang.String) */
    @Override
    public Map<Object, Object> getMap(String name) {
        return hazelcastInstance.getMap(name);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getLocalCache(java.lang.String) */
    @Override
    public Map<Object, Object> getLocalCache(String name) {
        // Hazelcast has no local cache implementation, so falling back to map
        return getMap(name);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#addToMap(java.lang.String, java.lang.Object,
     * java.lang.Object) */
    @Override
    public <K, V> V addToMap(String name, K key, V value) {
        return addToMap(name, key, value, 0, TimeUnit.SECONDS);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToMap(java.lang.String, java.lang.Object,
     * java.lang.Object, long, java.util.concurrent.TimeUnit) */
    @SuppressWarnings("unchecked")
    @Override
    public <K, V> V addToMap(String name, K key, V value, long ttl, TimeUnit timeUnit) {
        return (V) hazelcastInstance.getMap(name).put(key, value, ttl, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToSetMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Object) */
    @Override
    public <K, V> boolean addToSetMultiMap(String name, K key, V value) {
        return hazelcastInstance.getMultiMap(name).put(key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addAllToSetMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Iterable) */
    @Override
    public <K, V> boolean addAllToSetMultiMap(String name, K key, Iterable<? extends V> values) {
        if (values == null) {
            return false;
        }
        boolean result = false;
        MultiMap<K, V> map = hazelcastInstance.getMultiMap(name);
        for (V value : values) {
            result |= map.put(key, value);
        }
        return result;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToLocalCache(java.lang.String, java.lang.Object,
     * java.lang.Object) */
    @Override
    public <K, V> V addToLocalCache(String name, K key, V value) {
        return addToLocalCache(name, key, value, 0, TimeUnit.SECONDS);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToLocalCache(java.lang.String, java.lang.Object,
     * java.lang.Object, long, java.util.concurrent.TimeUnit) */
    @Override
    public <K, V> V addToLocalCache(String name, K key, V value, long ttl, TimeUnit timeUnit) {
        // Hazelcast has no local cache implementation, so falling back to map
        return addToMap(name, key, value, ttl, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getSetMultiMapItems(java.lang.String,
     * java.lang.Object) */
    @Override
    public Collection<?> getSetMultiMapItems(String name, Object key) {
        return hazelcastInstance.getMultiMap(name).get(key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromMap(java.lang.String, java.lang.Object) */
    public Object removeFromMap(String name, Object key) {
        return hazelcastInstance.getMap(name).remove(key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromSetMultiMap(java.lang.String,
     * java.lang.Object, java.lang.Object) */
    public boolean removeFromSetMultiMap(String name, Object key, Object value) {
        return hazelcastInstance.getMultiMap(name).remove(key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromLocalCache(java.lang.String,
     * java.lang.Object) */
    @Override
    public Object removeFromLocalCache(String name, Object key) {
        // Hazelcast has no local cache implementation, so falling back to map
        return removeFromMap(name, key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getSetMultiMapValueSize(java.lang.String,
     * java.lang.Object) */
    @Override
    public int getSetMultiMapValueSize(String name, Object key) {
        return hazelcastInstance.getMultiMap(name).get(key).size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearMap(java.lang.String) */
    @Override
    public void clearMap(String name) {
        hazelcastInstance.getMap(name).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearSetMultiMap(java.lang.String) */
    @Override
    public void clearSetMultiMap(String name) {
        hazelcastInstance.getMultiMap(name).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearSetMultiMapWithKey(java.lang.String,
     * java.lang.Object) */
    @Override
    public void clearSetMultiMapWithKey(String name, Object key) {
        hazelcastInstance.getMultiMap(name).get(key).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addItemToQueue(java.lang.String, java.lang.Object) */
    @Override
    public <T> boolean addItemToQueue(String name, T item) {
        String expirableMapName = DefneCacheConstants.EXPIRABLE_OBJECT_NAME_PREFIX + name;
        DistItemConfig<?, ?> config = expirableQueueMap.get(expirableMapName);

        /* If queue is expirable then add item to the map as well. */
        if (config != null) {
            // We use boolean value for performance issues
            IMap<T, Boolean> map = hazelcastInstance.getMap(expirableMapName);
            if (config.getTimeToLiveSeconds() > 0) {
                map.put(item, true, config.getTimeToLiveSeconds(), TimeUnit.SECONDS);
            }
        }

        IQueue<T> queue = hazelcastInstance.getQueue(name);
        return queue.offer(item);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addItemToQueue(java.lang.String, java.lang.Object, int,
     * java.util.concurrent.TimeUnit) */
    @Override
    public <T> void addItemToQueue(String name, T item, int delay, TimeUnit timeUnit) {
        // Add item to queue after the given delay
        hazelcastInstance.getScheduledExecutorService("SCHEDULER-" + name)
                        .scheduleOnMember(() -> addItemToQueue(name, item),
                                          hazelcastInstance.getCluster().getLocalMember(), delay,
                                          timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addItemToPriorityQueue(java.lang.String,
     * java.lang.Object, java.util.Comparator) */
    @Override
    public <T> boolean addItemToPriorityQueue(String name, T item, Comparator<T> comparator) {
        /* TODO: Hazelcast currently doesn't have a PriorityQueue mechanism, so we fallback to
         * reqular queue implementation. */
        return addItemToQueue(name, item);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addItemToPriorityQueue(java.lang.String,
     * java.lang.Object, java.util.Comparator, int, java.util.concurrent.TimeUnit) */
    @Override
    public <T> void addItemToPriorityQueue(String name,
                                           T item,
                                           Comparator<T> comparator,
                                           int delay,
                                           TimeUnit timeUnit) {
        addItemToQueue(name, item, delay, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#pollFromPriorityQueue(java.lang.String) */
    @Override
    public Object pollFromPriorityQueue(String queueName) {
        Queue<Object> queue = getPriorityQueue(queueName);
        if (queue.isEmpty())
            return null;

        PriorityQueue<Object> priorityQueue = new PriorityQueue<>(queue);
        Object item = priorityQueue.poll();
        if (queue.remove(item)) {
            return item;
        }
        logger.warn("Cannot poll item from distributed priority queue '{}'", queueName);
        return null;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getQueueSize(java.lang.String) */
    @Override
    public int getQueueSize(String queueName) {
        IQueue<?> queue = hazelcastInstance.getQueue(queueName);
        return queue.size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getPriorityQueueSize(java.lang.String) */
    @Override
    public int getPriorityQueueSize(String queueName) {
        /* TODO: Hazelcast currently doesn't have a PriorityQueue mechanism, so we fallback to
         * reqular queue implementation. */
        return getQueueSize(queueName);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getPriorityQueue(java.lang.String) */
    @Override
    public BlockingQueue<Object> getPriorityQueue(String queueName) {
        /* TODO: Hazelcast currently doesn't have a PriorityQueue mechanism, so we fallback to
         * reqular queue implementation. */
        return hazelcastInstance.getQueue(queueName);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromPriorityQueue(java.lang.String,
     * java.lang.Object) */
    @Override
    public boolean removeFromPriorityQueue(String queueName, Object item) {
        /* TODO: Hazelcast currently doesn't have a PriorityQueue mechanism, so we fallback to
         * reqular queue implementation. */
        return hazelcastInstance.getQueue(queueName).remove(item);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromQueue(java.lang.String, java.lang.Object) */
    @Override
    public int removeFromQueue(String queueName, Object item) {
        // If this queue is an expirable queue (backed by a map), then remove item from the map as well
        if (hasExpirableMapForObject(queueName)) {
            String mapName = getExpirableMapNameForObject(queueName);
            removeFromMap(mapName, item);
        }

        IQueue<Object> queue = hazelcastInstance.getQueue(queueName);
        // Find item index and return it
        Iterator<Object> iterator = queue.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            if (iterator.next().equals(item)) {
                break;
            }
            index++;
        }
        // Iterator.remove() is not supported in Hazelcast
        queue.remove(item);
        return index;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getTreeSetSize(java.lang.String) */
    @Override
    public int getTreeSetSize(String setName) {
        throw new RuntimeException("Set feature not implemented in Hazelcast!");
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getTreeSet(java.lang.String) */
    @Override
    public Iterable<Object> getTreeSet(String setName) {
        throw new RuntimeException("Set feature not implemented in Hazelcast!");
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromTreeSet(java.lang.String, java.lang.Object) */
    @Override
    public boolean removeFromTreeSet(String setName, Object item) {
        throw new RuntimeException("Set feature not implemented in Hazelcast!");
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getListMultiMapItems(java.lang.String,
     * java.lang.Object) */
    @Override
    public Collection<?> getListMultiMapItems(String name, Object key) {
        return getSetMultiMapItems(name, key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToListMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Object) */
    @Override
    public <K, V> boolean addToListMultiMap(String name, K key, V value) {
        return addToSetMultiMap(name, key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addAllToListMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Iterable) */
    @Override
    public <K, V> boolean addAllToListMultiMap(String name, K key, Iterable<? extends V> values) {
        return addAllToSetMultiMap(name, key, values);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromListMultiMap(java.lang.String,
     * java.lang.Object, java.lang.Object) */
    @Override
    public boolean removeFromListMultiMap(String name, Object key, Object value) {
        return removeFromSetMultiMap(name, key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getListMultiMapValueSize(java.lang.String,
     * java.lang.Object) */
    @Override
    public int getListMultiMapValueSize(String name, Object key) {
        return getSetMultiMapValueSize(name, key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearListMultiMap(java.lang.String) */
    @Override
    public void clearListMultiMap(String name) {
        clearSetMultiMap(name);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearListMultiMapWithKey(java.lang.String,
     * java.lang.Object) */
    @Override
    public void clearListMultiMapWithKey(String name, Object key) {
        clearListMultiMapWithKey(name, key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#setQueueTimeToLiveSeconds(java.lang.String, int) */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void setQueueConfig(String name, DistItemConfig config) {
        QueueConfig queueConfig = hazelcastInstance.getConfig().getQueueConfig(name);
        if (config.getMaxSize() > 0) {
            queueConfig.setMaxSize(config.getMaxSize());
        } else {
            queueConfig.setMaxSize(Integer.MAX_VALUE);
        }

        if (config.getTimeToLiveSeconds() > 0) {
            String mapName = getExpirableMapNameForObject(name);
            expirableQueueMap.put(mapName, config);

            if (config.getEntryExpiredListener() != null) {
                hazelcastInstance.getMap(mapName)
                                .addEntryListener((EntryExpiredListener<Object, Object>) event -> {
                                    int itemIndex = removeFromQueue(name, event.getKey());
                                    // First remove item from queue
                                    if (itemIndex != -1) {
                                        // Trigger event listener
                                        config.getEntryExpiredListener()
                                                        .entryExpired(new EntryEvent<Object, Object>(
                                                                        event.getKey(),
                                                                        event.getValue(),
                                                                        event.getOldValue(),
                                                                        itemIndex));
                                    } else {
                                        logger.warn("Item cannot be removed from distributed queue "
                                                    + "since it could not be found! Queue name: {}, "
                                                    + "Item: {}", name, event.getKey());
                                    }
                                }, false);
            }
        }
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#setMapConfig(java.lang.String,
     * com.cihangurkan.ccm.dist.DistItemConfig) */
    @Override
    public void setMapConfig(String name, DistItemConfig<Object, Object> config) {
        MapConfig mapConfig = hazelcastInstance.getConfig().getMapConfig(name);
        int maxSize = config.getMaxSize() > 0 ? config.getMaxSize() : Integer.MAX_VALUE;
        mapConfig.setMaxSizeConfig(new MaxSizeConfig(maxSize, MaxSizePolicy.PER_NODE));
        IMap<Object, Object> map = hazelcastInstance.getMap(name);

        if (config.getTimeToLiveSeconds() > 0) {
            mapConfig.setTimeToLiveSeconds(config.getTimeToLiveSeconds());
        }

        if (config.getEntryExpiredListener() != null) {
            map.addEntryListener((EntryExpiredListener<Object, Object>) event ->
            // Trigger event listener
            config.getEntryExpiredListener().entryExpired(new EntryEvent<Object, Object>(
                            event.getKey(), event.getValue(), event.getOldValue())), false);
        }
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getSet(java.lang.String) */
    @Override
    public Set<Object> getSet(String name) {
        return hazelcastInstance.getSet(name);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#startTransaction() */
    public Object startTransaction() {
        TransactionOptions options = new TransactionOptions()
                        .setTransactionType(TransactionType.ONE_PHASE);
        TransactionContext context = hazelcastInstance.newTransactionContext(options);
        context.beginTransaction();
        return context;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#commitTransaction(java.lang.Object) */
    public void commitTransaction(Object transaction) {
        TransactionContext context = (TransactionContext) transaction;
        context.commitTransaction();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#rollbackTransaction(java.lang.Object) */
    public void rollbackTransaction(Object transaction) {
        TransactionContext context = (TransactionContext) transaction;
        context.rollbackTransaction();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#cleanCache() */
    @Override
    public void cleanCache() {
        hazelcastInstance.getDistributedObjects().forEach(dist -> dist.destroy());
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getMemberCount() */
    @Override
    public int getMemberCount() {
        return hazelcastInstance.getCluster().getMembers().size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getAtomicLongValue(java.lang.String) */
    @Override
    public long incrementAndGetAtomicLong(String name) {
        return hazelcastInstance.getAtomicLong(name).incrementAndGet();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addAndGetAtomicLong(java.lang.String, long) */
    @Override
    public long addAndGetAtomicLong(String name, long delta) {
        return hazelcastInstance.getAtomicLong(name).addAndGet(delta);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getAtomicLong(java.lang.String) */
    @Override
    public long getAtomicLongValue(String name) {
        return hazelcastInstance.getAtomicLong(name).get();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#setAtomicLongValue(java.lang.String, long) */
    @Override
    public void setAtomicLongValue(String name, long value) {
        hazelcastInstance.getAtomicLong(name).set(value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#isMasterNode() */
    @Override
    public boolean isMasterNode() {
        return ((HazelcastInstanceImpl) hazelcastInstance).node.isMaster();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#scheduleTask(java.lang.String, java.lang.Runnable, long,
     * java.util.concurrent.TimeUnit) */
    @Override
    public ScheduledFuture<?> scheduleTask(Runnable task, long delay, TimeUnit timeUnit) {
        return hazelcastInstance.getScheduledExecutorService(TASK_EXECUTOR).schedule(task, delay,
                                                                                     timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#scheduleTask(java.lang.String,
     * java.util.concurrent.Callable, long, java.util.concurrent.TimeUnit) */
    @Override
    public ScheduledFuture<?> scheduleTask(Callable<?> task, long delay, TimeUnit timeUnit) {
        return hazelcastInstance.getScheduledExecutorService(TASK_EXECUTOR).schedule(task, delay,
                                                                                     timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#cancelTask(java.util.concurrent.ScheduledFuture,
     * boolean) */
    @Override
    public boolean cancelTask(ScheduledFuture<?> future, boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#shutdown() */
    @Override
    public void shutdown() {
        hazelcastInstance.getScheduledExecutorService(TASK_EXECUTOR).shutdown();
        hazelcastInstance.shutdown();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#registerRemoteService(java.lang.Class,
     * java.lang.Object) */
    @Override
    public <T> void registerRemoteService(Class<T> remoteInterface, T object) {
        // Not implemented
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getRemoteService(java.lang.Class) */
    @Override
    public <T> T getRemoteService(Class<T> remoteInterface) {
        // Workaround to simulate the same like Redisson
        return applicationContext.getBean(remoteInterface);
    }

    @Override
    public List<String> getKeys() {
        List<String> list = new ArrayList<>();
        hazelcastInstance.getDistributedObjects().forEach(obj -> list.add(obj.getName()));
        return list;
    }
}
