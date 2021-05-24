package com.cihangurkan.cache.redis.concrete;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.redisson.RedissonNode;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RBoundedBlockingQueue;
import org.redisson.api.RLock;
import org.redisson.api.RMapCache;
import org.redisson.api.RPriorityBlockingQueue;
import org.redisson.api.RRemoteService;
import org.redisson.api.RTransaction;
import org.redisson.api.RedissonClient;
import org.redisson.api.TransactionOptions;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.client.RedisConnection;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.config.RedissonNodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cihangurkan.cache.abstracts.CacheManager;
import com.cihangurkan.cache.common.DefneCacheConstants;
import com.cihangurkan.cache.concrete.DistItemConfig;
import com.cihangurkan.cache.concrete.EntryEvent;

/**
 * Redisson cache implementation of {@link CacheManager} interface.
 * 
 * @author Okan ARDIC
 *
 */
public class RedissonCacheManager extends CacheManager {

    private static Logger logger = LoggerFactory.getLogger(RedissonCacheManager.class);

    private RedissonClient redissonClient;
    private RedisConnection nativeConnection;
    private RedissonProperties redissonProperties;

    public RedissonCacheManager(RedissonClient redissonClient,
                                RedisConnection nativeConnection,
                                RedissonProperties redissonProperties) {
        this.redissonClient = redissonClient;
        this.nativeConnection = nativeConnection;
        this.redissonProperties = redissonProperties;

        initExecutorService(TASK_EXECUTOR);
    }

    /**
     * Initializes the {@link ExecutorService} implementation for the given executor name. If this
     * method is called once, subsequent calls will have no effect.
     * 
     * @param executorName
     */
    private void initExecutorService(String executorName) {
        RedissonNodeConfig nodeConfig = new RedissonNodeConfig(redissonClient.getConfig());
        nodeConfig.setExecutorServiceWorkers(Collections.singletonMap(executorName, 10));
        RedissonNode node = RedissonNode.create(nodeConfig, redissonClient);
        node.start();
    }

    //    public void setRedissonClient(RedissonClient client) {
    //        redissonClient = client;
    //    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#lock(java.lang.String) */
    @Override
    public void lock(String lockKey) {
        redissonClient.getLock(lockKey).lock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#lock(java.lang.String, int) */
    @Override
    public void lock(String lockKey, int leaseTime) {
        lock(lockKey, TimeUnit.SECONDS, leaseTime);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#lock(java.lang.String, java.util.concurrent.TimeUnit,
     * int) */
    @Override
    public void lock(String lockKey, TimeUnit unit, int leaseTime) {
        redissonClient.getLock(lockKey).lock(leaseTime, unit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#tryLock(java.lang.String) */
    @Override
    public boolean tryLock(String lockKey) {
        return redissonClient.getLock(lockKey).tryLock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#tryLock(java.lang.String, int, int) */
    @Override
    public boolean tryLock(String lockKey, int waitTime, int leaseTime) {
        return tryLock(lockKey, TimeUnit.SECONDS, waitTime, leaseTime);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#tryLock(java.lang.String, java.util.concurrent.TimeUnit,
     * int, int) */
    @Override
    public boolean tryLock(String lockKey, TimeUnit unit, int waitTime, int leaseTime) {
        try {
            return redissonClient.getLock(lockKey).tryLock(waitTime, leaseTime, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#unlock(java.lang.String) */
    @Override
    public void unlock(String lockKey) {
        RLock lock = redissonClient.getLock(lockKey);
        if (lock.isHeldByCurrentThread())
            lock.unlock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#forceUnlock(java.lang.String) */
    @Override
    public void forceUnlock(String key) {
        redissonClient.getLock(key).forceUnlock();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheUtil#getMapCache(java.lang.String) */
    @Override
    public Map<Object, Object> getMap(String name) {
        return redissonClient.getMapCache(name);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getLocalCache(java.lang.String) */
    @Override
    public Map<Object, Object> getLocalCache(String name) {
        return redissonClient.getLocalCachedMap(name, LocalCachedMapOptions.defaults());
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
        return (V) redissonClient.getMapCache(name).put(key, value, ttl, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToSetMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Object) */
    @Override
    public <K, V> boolean addToSetMultiMap(String name, K key, V value) {
        return redissonClient.getSetMultimap(name).put(key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addAllToSetMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Iterable) */
    @Override
    public <K, V> boolean addAllToSetMultiMap(String name, K key, Iterable<? extends V> values) {
        return redissonClient.getSetMultimap(name).putAll(key, values);
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
    @SuppressWarnings("unchecked")
    public <K, V> V addToLocalCache(String name, K key, V value, long ttl, TimeUnit timeUnit) {
        return (V) redissonClient
                        .getLocalCachedMap(name,
                                           LocalCachedMapOptions.defaults().timeToLive(ttl,
                                                                                       timeUnit))
                        .put(key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromMap(java.lang.String, java.lang.Object) */
    @Override
    public Object removeFromMap(String name, Object key) {
        return redissonClient.getMapCache(name).remove(key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromSetMultiMap(java.lang.String,
     * java.lang.Object, java.lang.Object) */
    @Override
    public boolean removeFromSetMultiMap(String name, Object key, Object value) {
        return redissonClient.getSetMultimap(name).remove(key, value);
    }

    @Override
    public Object removeFromLocalCache(String name, Object key) {
        return redissonClient.getLocalCachedMap(name, LocalCachedMapOptions.defaults()).remove(key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getSetMultiMapItems(java.lang.String,
     * java.lang.Object) */
    @Override
    public Collection<?> getSetMultiMapItems(String name, Object key) {
        return redissonClient.getSetMultimap(name).getAll(key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getSetMultiMapValueSize(java.lang.String,
     * java.lang.Object) */
    @Override
    public int getSetMultiMapValueSize(String name, Object key) {
        return redissonClient.getSetMultimap(name).get(key).size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearMap(java.lang.String) */
    @Override
    public void clearMap(String name) {
        redissonClient.getMapCache(name).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearSetMultiMap(java.lang.String) */
    @Override
    public void clearSetMultiMap(String name) {
        redissonClient.getSetMultimap(name).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearSetMultiMapWithKey(java.lang.String,
     * java.lang.Object) */
    @Override
    public void clearSetMultiMapWithKey(String name, Object key) {
        redissonClient.getSetMultimap(name).get(key).clear();
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
            RMapCache<Object, T> map = redissonClient.getMapCache(expirableMapName);
            if (config.getTimeToLiveSeconds() > 0) {
                map.fastPut(map.size(), item, config.getTimeToLiveSeconds(), TimeUnit.SECONDS);
            }
        }
        RBlockingQueue<T> queue = redissonClient.getBoundedBlockingQueue(name);
        return queue.offer(item);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addItemToQueue(java.lang.String,
     * com.cihangurkan.ccm.dist.PriorityAgent, int, java.util.concurrent.TimeUnit) */
    @Override
    public <T> void addItemToQueue(String name, T item, int delay, TimeUnit timeUnit) {
        String expirableMapName = DefneCacheConstants.EXPIRABLE_OBJECT_NAME_PREFIX + name;
        DistItemConfig<?, ?> config = expirableQueueMap.get(expirableMapName);

        /* If queue is expirable then add item to the map as well. */
        if (config != null) {
            // We use boolean value for performance issues
            RMapCache<T, Boolean> map = redissonClient.getMapCache(expirableMapName);
            if (config.getTimeToLiveSeconds() > 0) {
                map.fastPut(item, true, config.getTimeToLiveSeconds(), TimeUnit.SECONDS);
            }
        }
        RBlockingQueue<T> queue = redissonClient.getBoundedBlockingQueue(name);
        redissonClient.getDelayedQueue(queue).offer(item, delay, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addItemToPriorityQueue(java.lang.String,
     * java.lang.Object, java.util.Comparator) */
    @Override
    public <T> boolean addItemToPriorityQueue(String name, T value, Comparator<T> comparator) {
        RPriorityBlockingQueue<T> queue = redissonClient.getPriorityBlockingQueue(name);
        queue.trySetComparator(comparator);
        return queue.offer(value);
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
        RPriorityBlockingQueue<T> queue = redissonClient.getPriorityBlockingQueue(name);
        queue.trySetComparator(comparator);
        redissonClient.getDelayedQueue(queue).offerAsync(item, delay, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#pollFromPriorityQueue(java.lang.String) */
    @Override
    public Object pollFromPriorityQueue(String queueName) {
        return getPriorityQueue(queueName).poll();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getQueueSize(java.lang.String) */
    @Override
    public int getQueueSize(String queueName) {
        RBlockingQueue<?> queue = redissonClient.getBoundedBlockingQueue(queueName);
        return queue.size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getPriorityQueueSize(java.lang.String) */
    @Override
    public int getPriorityQueueSize(String queueName) {
        RBlockingQueue<?> queue = redissonClient.getPriorityBlockingQueue(queueName);
        return queue.size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getPriorityQueue(java.lang.String) */
    @Override
    public BlockingQueue<Object> getPriorityQueue(String queueName) {
        return redissonClient.getPriorityBlockingQueue(queueName);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromPriorityQueue(java.lang.String,
     * java.lang.Object) */
    @Override
    public boolean removeFromPriorityQueue(String queueName, Object item) {
        return redissonClient.getPriorityBlockingQueue(queueName).remove(item);
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

        // Find item index and return it
        Iterator<Object> iterator = redissonClient.getBoundedBlockingQueue(queueName).iterator();
        int index = -1;
        while (iterator.hasNext()) {
            if (iterator.next().equals(item)) {
                iterator.remove();
                break;
            }
            index++;
        }
        return index + 1;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getTreeSetSize(java.lang.String) */
    @Override
    public int getTreeSetSize(String setName) {
        return redissonClient.getScoredSortedSet(setName).size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getTreeSet(java.lang.String) */
    @Override
    public Iterable<Object> getTreeSet(String setName) {
        return redissonClient.getScoredSortedSet(setName);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromTreeSet(java.lang.String, java.lang.Object) */
    @Override
    public boolean removeFromTreeSet(String setName, Object item) {
        return redissonClient.getScoredSortedSet(setName).remove(item);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getListMultiMapItems(java.lang.String,
     * java.lang.Object) */
    @Override
    public Collection<?> getListMultiMapItems(String name, Object key) {
        return redissonClient.getListMultimap(name).getAll(key);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addToListMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Object) */
    @Override
    public <K, V> boolean addToListMultiMap(String name, K key, V value) {
        return redissonClient.getListMultimap(name).put(key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addAllToListMultiMap(java.lang.String, java.lang.Object,
     * java.lang.Iterable) */
    @Override
    public <K, V> boolean addAllToListMultiMap(String name, K key, Iterable<? extends V> values) {
        return redissonClient.getSetMultimap(name).putAll(key, values);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#removeFromListMultiMap(java.lang.String,
     * java.lang.Object, java.lang.Object) */
    @Override
    public boolean removeFromListMultiMap(String name, Object key, Object value) {
        return redissonClient.getListMultimap(name).remove(key, value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getListMultiMapValueSize(java.lang.String,
     * java.lang.Object) */
    @Override
    public int getListMultiMapValueSize(String name, Object key) {
        return redissonClient.getListMultimap(name).get(key).size();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearListMultiMap(java.lang.String) */
    @Override
    public void clearListMultiMap(String name) {
        redissonClient.getListMultimap(name).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#clearListMultiMapWithKey(java.lang.String,
     * java.lang.Object) */
    @Override
    public void clearListMultiMapWithKey(String name, Object key) {
        redissonClient.getListMultimap(name).get(key).clear();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#setQueueConfig(java.lang.String,
     * com.cihangurkan.ccm.dist.DistItemConfig) */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void setQueueConfig(String name, DistItemConfig config) {
        RBoundedBlockingQueue<?> queue = redissonClient.getBoundedBlockingQueue(name);
        if (config.getMaxSize() > 0) {
            queue.trySetCapacity(config.getMaxSize());
        } else {
            queue.trySetCapacity(Integer.MAX_VALUE);
        }
        if (config.getTimeToLiveSeconds() > 0) {
            String mapName = DefneCacheConstants.EXPIRABLE_OBJECT_NAME_PREFIX + name;
            expirableQueueMap.put(mapName, config);

            if (config.getEntryExpiredListener() != null) {
                redissonClient.getMapCache(mapName)
                                .addListener((EntryExpiredListener<Object, Object>) event -> {
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
                                });
            }
        }
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#setMapConfig(java.lang.String,
     * com.cihangurkan.ccm.dist.DistItemConfig) */
    @Override
    public void setMapConfig(String name, DistItemConfig<Object, Object> config) {
        RMapCache<?, ?> map = redissonClient.getMapCache(name);
        if (config.getMaxSize() > 0) {
            map.setMaxSize(config.getMaxSize());
        } else {
            map.setMaxSize(Integer.MAX_VALUE);
        }
        if (config.getTimeToLiveSeconds() > 0 && config.getEntryExpiredListener() != null) {
            map.addListener((EntryExpiredListener<Object, Object>) event ->
            // Trigger event listener
            config.getEntryExpiredListener().entryExpired(new EntryEvent<Object, Object>(
                            event.getKey(), event.getValue(), event.getOldValue())));
        }
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getSet(java.lang.String) */
    @Override
    public Set<Object> getSet(String name) {
        return redissonClient.getSet(name);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#startTransaction() */
    public Object startTransaction() {
        return redissonClient.createTransaction(TransactionOptions.defaults());
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#commitTransaction(java.lang.Object) */
    public void commitTransaction(Object transaction) {
        RTransaction rTransaction = (RTransaction) transaction;
        rTransaction.commit();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#rollbackTransaction(java.lang.Object) */
    public void rollbackTransaction(Object transaction) {
        RTransaction rTransaction = (RTransaction) transaction;
        rTransaction.rollback();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#cleanCache() */
    @Override
    public void cleanCache() {
        redissonClient.getKeys().flushdb();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getMemberCount() */
    @Override
    public int getMemberCount() {
        //        return redissonClient.getNodesGroup().getNodes().size();

        //        RScript script = redisson.getScript(StringCodec.INSTANCE);
        //
        //        List<Object> = script.eval(Mode.READ_ONLY,
        //                               "return redis.call('get', 'foo')", 
        //                               RScript.ReturnType.MULTI);

        List<Object> clients = nativeConnection.sync(StringCodec.INSTANCE,
                                                     RedisCommands.CLIENT_LIST);
        return Math.round((float) ((clients.size() - 1)
                                   / (float) (redissonProperties.getConnectionMinimumIdleSize()
                                              + 2)));
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#incrementAndGetAtomicLong(java.lang.String) */
    @Override
    public long incrementAndGetAtomicLong(String name) {
        return redissonClient.getAtomicLong(name).incrementAndGet();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#addAndGetAtomicLong(java.lang.String, long) */
    @Override
    public long addAndGetAtomicLong(String name, long delta) {
        return redissonClient.getAtomicLong(name).addAndGet(delta);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#getAtomicLongValue(java.lang.String) */
    @Override
    public long getAtomicLongValue(String name) {
        return redissonClient.getAtomicLong(name).get();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#setAtomicLongValue(java.lang.String, long) */
    @Override
    public void setAtomicLongValue(String name, long value) {
        redissonClient.getAtomicLong(name).set(value);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#isMasterNode() */
    @Override
    public boolean isMasterNode() {
        // TODO Auto-generated method stub
        return true;
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#scheduleTask(java.lang.String, java.lang.Runnable, long,
     * java.util.concurrent.TimeUnit) */
    @Override
    public ScheduledFuture<?> scheduleTask(Runnable task, long delay, TimeUnit timeUnit) {
        return redissonClient.getExecutorService(TASK_EXECUTOR).schedule(task, delay, timeUnit);
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#scheduleTask(java.lang.String,
     * java.util.concurrent.Callable, long, java.util.concurrent.TimeUnit) */
    @Override
    public ScheduledFuture<?> scheduleTask(Callable<?> task, long delay, TimeUnit timeUnit) {
        return redissonClient.getExecutorService(TASK_EXECUTOR).schedule(task, delay, timeUnit);
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
        redissonClient.getExecutorService(TASK_EXECUTOR).shutdown();
        redissonClient.shutdown();
    }

    /* (non-Javadoc)
     * 
     * @see com.cihangurkan.ccm.dist.CacheManager#registerRemoteService(java.lang.Class,
     * java.lang.Object) */
    @Override
    public <T> void registerRemoteService(Class<T> remoteInterface, T object) {
        redissonClient.getRemoteService().register(remoteInterface, object);
    }

    @Override
    public <T> T getRemoteService(Class<T> remoteInterface) {
        RRemoteService remoteService = redissonClient.getRemoteService();
        return remoteService.get(remoteInterface);
    }

    @Override
    public List<String> getKeys() {
        return redissonClient.getKeys().getKeysStream().collect(Collectors.toList());
    }
}
