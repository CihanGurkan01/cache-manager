package com.cihangurkan.cache.abstracts;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.cihangurkan.cache.common.DefneCacheConstants;
import com.cihangurkan.cache.concrete.DistItemConfig;

/**
 * Main interface to handle different distributed cache implementations such as Hazelcast, Redis
 * etc.
 * 
 * @author Okan ARDIC
 *
 */
public abstract class CacheManager {

    /**
     * Name of the {@link ExecutorService} to execute distributed tasks.
     */
    protected static final String TASK_EXECUTOR = "TASK_EXECUTOR";

    /**
     * Stores {@code <Queue Name, DistItemConfig>} pairs with time-to-live feature. Hazelcast and
     * Redis queues do not support expiration for entries, so this is a workaround solution.
     */
    @SuppressWarnings("rawtypes")
    protected static Map<String, DistItemConfig> expirableQueueMap = new ConcurrentHashMap<>();

    /**
     * Checks whether there's an expirable map for the distributed object identified by the given
     * name.
     * 
     * @param name distributed object name
     * @return
     */
    public boolean hasExpirableMapForObject(String name) {
        String mapName = getExpirableMapNameForObject(name);
        return expirableQueueMap.containsKey(mapName);
    }

    /**
     * Checks and retrieves the expirable map for the distributed object identified by the given
     * name.
     * 
     * @param name distributed object name
     * @return expirable map if found, otherwise null
     */
    public Map<Object, Object> getExpirableMapForObject(String name) {
        String mapName = getExpirableMapNameForObject(name);
        if (expirableQueueMap.containsKey(mapName)) {
            return getMap(mapName);
        }
        return null;
    }

    /**
     * Retrieves expirable map name for the distributed object identified by the given name.
     * 
     * @param name name of the distributed object to get associated map name
     * @return expirable map name for the distributed object identified by the given name
     */
    public String getExpirableMapNameForObject(String name) {
        return DefneCacheConstants.EXPIRABLE_OBJECT_NAME_PREFIX + name;
    }

    /**
     * Locks distributed cache associated to the given key to be accessed by other threads and
     * processes.
     */
    public abstract void lock(String key);

    /**
     * Locks distributed cache to be accessed by other threads and processes for the given time
     * period in seconds.
     * 
     * @param key
     * @param leaseTime
     */
    public abstract void lock(String key, int leaseTime);

    /**
     * Locks distributed cache associated to the given key to be accessed by other threads and
     * processes for the given time
     * period in given {@link TimeUnit}.
     * 
     * @param key
     * @param timeUnit
     * @param leaseTime
     */
    public abstract void lock(String key, TimeUnit timeUnit, int leaseTime);

    /**
     * Tries to lock the cache associated to the given key and returns the result immediately.
     * 
     * @param key
     * @return true if operation is successful, false if another process/thread already holds the
     *         lock.
     */
    public abstract boolean tryLock(String key);

    /**
     * Tries to lock the cache associated to the given key for the given time period in seconds. If
     * lock is acquired successfully, then it will be held for the given lease time period in
     * seconds.
     * 
     * @param key
     * @param waitTime
     * @param leaseTime
     * @return true if operation is successful, false if lock could not be acquired within the
     *         specified time period.
     */
    public abstract boolean tryLock(String key, int waitTime, int leaseTime);

    /**
     * Tries to lock the cache associated to the given key for the given time period in seconds. If
     * lock is acquired successfully, then it will be held for the given lease time period in
     * in given {@link TimeUnit}.
     * 
     * @param key
     * @param timeUnit
     * @param waitTime
     * @param leaseTime
     * @return true if operation is successful, false if lock could not be acquired within the
     *         specified time period.
     */
    public abstract boolean tryLock(String key, TimeUnit timeUnit, int waitTime, int leaseTime);

    /**
     * Unlocks the distributed cache associated to the given key to allow access to other threads
     * and processes.
     * This method has no effect, if lock is held by other cache member.
     */
    public abstract void unlock(String key);

    /**
     * Releases the lock regardless of the lock owner. It always successfully unlocks, never blocks,
     * and returns immediately.
     */
    public abstract void forceUnlock(String key);

    /**
     * Retrieves the map from the cache with the given name.
     * 
     * @param name name of the map
     * @return the map from the cache with the given name
     */
    public abstract Map<Object, Object> getMap(String name);

    /**
     * Retrieves the local cache with the given name.
     * 
     * @param name name of the local cache.
     * @return the local cache with the given name
     */
    public abstract Map<Object, Object> getLocalCache(String name);

    /**
     * Puts the given key-value pair to the map defined by the given name.
     * 
     * @param name name of the map
     * @param key key to add
     * @param value value to associate with the given key
     * @return previous value associated with key or null if there was no mapping for key.
     */
    public abstract <K, V> V addToMap(String name, K key, V value);

    /**
     * Puts the given key-value pair to the map defined by the given name and time-to-live period
     * with the given time unit.
     * 
     * @param name name of the map
     * @param key key to add
     * @param value value to associate with the given key
     * @return previous value associated with key or null if there was no mapping for key.
     */
    public abstract <K, V> V addToMap(String name, K key, V value, long ttl, TimeUnit timeUnit);

    /**
     * Removes key from the map with the given name and returns associated value.
     * 
     * @param name name of the map
     * @param key key to delete
     * @return deleted value, null if there wasn't any association
     */
    public abstract Object removeFromMap(String name, Object key);

    /**
     * Removes key from the local cache with the given name and returns associated value.
     * 
     * @param name name of the cache
     * @param key key to delete
     * @return deleted value, null if there wasn't any association
     */
    public abstract Object removeFromLocalCache(String name, Object key);

    /**
     * Clears the map identified by the given name.
     * 
     * @param name name of the map
     */
    public abstract void clearMap(String name);

    /**
     * Retrieves the collection value of the multi-map with the given name and map key.
     * 
     * @param name
     * @return
     */
    public abstract Collection<?> getSetMultiMapItems(String name, Object key);

    /**
     * Puts the given key-value pair to the multi-map defined by the given name.
     * 
     * @param name
     * @param key
     * @param value
     * @return true if size of the multimap is increased, false if the multimap already contains the
     *         key-value pair.
     */
    public abstract <K, V> boolean addToSetMultiMap(String name, K key, V value);

    /**
     * Puts the given key and list of values to the multi-map defined by the given name.
     * 
     * @param name
     * @param key
     * @param values
     * @return true if the multimap changed
     */
    public abstract <K, V> boolean addAllToSetMultiMap(String name,
                                                       K key,
                                                       Iterable<? extends V> values);

    /**
     * Puts the given key-value pair to the local cache defined by the given name.
     * 
     * @param name
     * @param key
     * @param value
     * @return previous value associated with key or null if there was no mapping for key.
     */
    public abstract <K, V> V addToLocalCache(String name, K key, V value);

    /**
     * Puts the given key-value pair to the local cache defined by the given name. Items will be
     * evicted after the given time-to-live value expires.
     * 
     * @param name
     * @param key
     * @param value
     * @param ttl
     * @param timeUnit
     * @return previous value associated with key or null if there was no mapping for key.
     */
    public abstract <K, V> V addToLocalCache(String name,
                                             K key,
                                             V value,
                                             long ttl,
                                             TimeUnit timeUnit);

    /**
     * Removes a single key-value pair with the key key and the value value from this multimap, if
     * such exists. If multiple key-value pairs in the multimap fit this description, which one is
     * removed is unspecified.
     * 
     * @param name map name
     * @param key map key
     * @param value map value
     * @return true if the multimap changed
     */
    public abstract boolean removeFromSetMultiMap(String name, Object key, Object value);

    /**
     * Returns the number of elements in the given multi-map associated to the given key.
     * 
     * @param name name of the multi-map to retrieve the size
     * @param key map key to retrieve the item count associated to it
     * @return the number of elements in the multi-map associated to the given key.
     */
    public abstract int getSetMultiMapValueSize(String name, Object key);

    /**
     * Clears the multi-map identified by the given name.
     * 
     * @param name name of the map
     */
    public abstract void clearSetMultiMap(String name);

    /**
     * Clears the values of the multi-map identified by the given name and map key.
     * 
     * @param name name of the map
     * @param key map key whose values to be cleared
     */
    public abstract void clearSetMultiMapWithKey(String name, Object key);

    /**
     * Retrieves the collection value of the multi-map with the given name and map key.
     * 
     * @param name
     * @return
     */
    public abstract Collection<?> getListMultiMapItems(String name, Object key);

    /**
     * Puts the given key-value pair to the multi-map defined by the given name.
     * 
     * @param name
     * @param key
     * @param value
     * @return true if size of the multimap is increased, false if the multimap already contains the
     *         key-value pair.
     */
    public abstract <K, V> boolean addToListMultiMap(String name, K key, V value);

    /**
     * Puts the given key and list of values to the multi-map defined by the given name.
     * 
     * @param name
     * @param key
     * @param values
     * @return true if the multimap changed
     */
    public abstract <K, V> boolean addAllToListMultiMap(String name,
                                                        K key,
                                                        Iterable<? extends V> values);

    /**
     * Removes a single key-value pair with the key key and the value value from this multimap, if
     * such exists. If multiple key-value pairs in the multimap fit this description, which one is
     * removed is unspecified.
     * 
     * @param name map name
     * @param key map key
     * @param value map value
     * @return true if the multimap changed
     */
    public abstract boolean removeFromListMultiMap(String name, Object key, Object value);

    /**
     * Returns the number of elements in the given multi-map associated to the given key.
     * 
     * @param name name of the multi-map to retrieve the size
     * @param key map key to retrieve the item count associated to it
     * @return the number of elements in the multi-map associated to the given key.
     */
    public abstract int getListMultiMapValueSize(String name, Object key);

    /**
     * Clears the multi-map identified by the given name.
     * 
     * @param name name of the map
     */
    public abstract void clearListMultiMap(String name);

    /**
     * Clears the values of the multi-map identified by the given name and map key.
     * 
     * @param name name of the map
     * @param key map key whose values to be cleared
     */
    public abstract void clearListMultiMapWithKey(String name, Object key);

    /**
     * Adds the given item to the end of the queue defined by the given name.
     * 
     * @param name name of the queue
     * @param item item to add to the queue
     * @return true if item has been successfully added to the queue, false otherwise. Remember that
     *         depending on the memory limit item might not be added to the queue.
     */
    public abstract <T> boolean addItemToQueue(String name, T item);

    /**
     * Adds the given item to the end of the queue after the given delay.
     * 
     * @param name name of the queue
     * @param item item to add to the queue
     * @param delay delay period
     * @param timeUnit delay time unit
     */
    public abstract <T> void addItemToQueue(String name, T item, int delay, TimeUnit timeUnit);

    /**
     * Adds the given item to the priority queue defined by the given name.
     * 
     * @param name name of the queue
     * @param item item to add to the queue
     * @param comparator {@link Comparator} instance to compare the queue items. Sorting will be
     *            done according to the given comparator. This parameter has no effect, if a
     *            comparator for the queue had already been assigned.
     * @return true if item has been successfully added to the queue, false otherwise. Remember that
     *         depending on the memory limit the item might not be added to the queue.
     */
    public abstract <T> boolean addItemToPriorityQueue(String name,
                                                       T item,
                                                       Comparator<T> comparator);

    /**
     * @param name name of the queue
     * @param item item to add to the queue
     * @param comparator {@link Comparator} instance to compare the queue items. Sorting will be
     *            done according to the given comparator. This parameter has no effect, if a
     *            comparator for the queue had already been assigned.
     * @param delay delay period
     * @param timeUnit delay time unit
     */
    public abstract <T> void addItemToPriorityQueue(String name,
                                                    T item,
                                                    Comparator<T> comparator,
                                                    int delay,
                                                    TimeUnit timeUnit);

    /**
     * Retrieves the size of the queue identified by the given queue name.
     * 
     * @param queueName name of the queue
     * @return the size of the queue identified by the given queue name
     */
    public abstract int getQueueSize(String queueName);

    /**
     * Retrieves the size of the priority queue identified by the given queue name.
     * 
     * @param queueName name of the queue
     * @return the size of the priority queue identified by the given queue name
     */
    public abstract int getPriorityQueueSize(String queueName);

    /**
     * Retrieves the priority queue identified by the given queue name.
     * 
     * @param queueName name of the queue
     * @return
     */
    public abstract BlockingQueue<Object> getPriorityQueue(String queueName);

    /**
     * Polls the first item from the priority queue identified by the given queue name.
     * 
     * @param queueName name of the queue
     * @return the head of this queue, or returns null if this queue is empty.
     */
    public abstract Object pollFromPriorityQueue(String queueName);

    /**
     * Removes the given item from the priority queue identified by the given queue name.
     * 
     * @param queueName name of the queue
     * @param item item to remove from the queue
     * @return true if item is removed from the queue, false otherwise
     */
    public abstract boolean removeFromPriorityQueue(String queueName, Object item);

    /**
     * Removes the given item from the queue identified by the given queue name and returns the item
     * index if found. Returns -1 if the given item is not found in the queue.
     * 
     * @param queueName name of the queue
     * @param item
     * @return
     */
    public abstract int removeFromQueue(String queueName, Object item);

    /**
     * Retrieves the size of the priority queue identified by the given queue name.
     * 
     * @param setName name of the set
     * @return the size of the priority queue identified by the given queue name
     */
    public abstract int getTreeSetSize(String setName);

    /**
     * Retrieves the tree set identified by the given queue name.
     * 
     * @param setName name of the set
     * @return set instance.
     * @see {@link TreeSet}
     */
    public abstract Iterable<Object> getTreeSet(String setName);

    /**
     * Removes the given item from the tree set identified by the given set name.
     * 
     * @param setName
     * @param item
     * @return true if item is removed from the set, false otherwise
     */
    public abstract boolean removeFromTreeSet(String setName, Object item);

    /**
     * Sets time-to-live value and max size for the queue identified by the given queue name.
     * 
     * @param name name of the queue
     * @param config configuration instance
     */
    public abstract void setQueueConfig(String name, DistItemConfig<?, ?> config);

    /**
     * Sets max size and {@link EntryExpiredListener} for the map identified by the given name.
     * 
     * @param name name of the map
     * @param config configuration instance
     */
    public abstract void setMapConfig(String name, DistItemConfig<Object, Object> config);

    /**
     * Retrieves the distributed set with the given name.
     * 
     * @param name name of the set
     * @return the distributed set with the given name
     */
    public abstract Set<Object> getSet(String name);

    /**
     * Starts a new transaction.
     * 
     * @return transaction instance
     */
    public abstract Object startTransaction();

    /**
     * Commits all changes within the given transaction.
     * 
     * @param transaction transaction to commit
     */
    public abstract void commitTransaction(Object transaction);

    /**
     * Rolls back all changes within the transaction.
     * 
     * @param transaction transaction to rollback
     */
    public abstract void rollbackTransaction(Object transaction);

    /**
     * Removes all items from the cache.
     */
    public abstract void cleanCache();

    /**
     * Retrieves the number of members connected to the cluster.
     * 
     * @return the number of members connected to the cluster.
     */
    public abstract int getMemberCount();

    /**
     * Increments {@code AtomicLong} instance with the given name and retrieves the new value.
     * 
     * @param name name of the {@code AtomicLong}
     * @return the distributed {@code AtomicLong} with the given name
     */
    public abstract long incrementAndGetAtomicLong(String name);

    /**
     * Increments {@code AtomicLong} instance with the given name and delta, and retrieves the new
     * value.
     * 
     * @param name name of the {@code AtomicLong}
     * @return the distributed {@code AtomicLong} with the given name
     */
    public abstract long addAndGetAtomicLong(String name, long delta);

    /**
     * Retrieves the new value of {@code AtomicLong} instance with the given name.
     * 
     * @param name name of the {@code AtomicLong}
     * @return the new value of {@code AtomicLong} instance with the given name.
     */
    public abstract long getAtomicLongValue(String name);

    /**
     * Sets the new value of {@code AtomicLong} instance with the given name.
     * 
     * @param name name of the {@code AtomicLong}
     * @param value value to set to {@code AtomicLong}
     */
    public abstract void setAtomicLongValue(String name, long value);

    /**
     * Retrieves whether this application represents the master node or not.
     * 
     * @return true if this application represents the master node, otherwise returns false.
     */
    public abstract boolean isMasterNode();

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param task the task to execute
     * @param delay the time from now to delay execution
     * @param timeUnit the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()}
     *         method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException if command is null
     */
    public abstract ScheduledFuture<?> scheduleTask(Runnable task, long delay, TimeUnit timeUnit);

    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param task the task to execute
     * @param delay the time from now to delay execution
     * @param timeUnit the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()}
     *         method will return {@code null} upon completion
     * @throws RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException if command is null
     */
    public abstract ScheduledFuture<?> scheduleTask(Callable<?> task,
                                                    long delay,
                                                    TimeUnit timeUnit);

    /**
     * Attempts to cancel execution of this task. This attempt will fail if the task has already
     * completed, has already been cancelled, or could not be cancelled for some other reason. If
     * successful, and this task has not started when cancel is called, this task should never run.
     * If the task has already started, then the mayInterruptIfRunning parameter determines whether
     * the thread executing this task should be interrupted in an attempt to stop the task.
     * 
     * After this method returns, subsequent calls to isDone will always return true. Subsequent
     * calls to isCancelled will always return true if this method returned true.
     * 
     * @param future {@link ScheduledFuture} to be cancelled
     * @param mayInterruptIfRunning true if the thread executing this task should be interrupted;
     *            otherwise, in-progress tasks are allowed to complete
     * @return false if the task could not be cancelled, typically because it has already completed
     *         normally; true otherwise
     * 
     * @see ScheduledFuture#cancel(boolean)
     */
    public abstract boolean cancelTask(ScheduledFuture<?> future, boolean mayInterruptIfRunning);

    /**
     * Takes necessary actions to shutdown the cache implementation
     */
    public abstract void shutdown();

    /**
     * Registers the given class instance for remote method invocation (RMI).
     * 
     * @param remoteInterface remote interface
     * @param object remote service implementation
     */
    public abstract <T> void registerRemoteService(Class<T> remoteInterface, T object);

    /**
     * Retrieves remote service implementation.
     * 
     * @param remoteInterface remote interface
     * @return remote service implementation
     */
    public abstract <T> T getRemoteService(Class<T> remoteInterface);

    /**
     * Retrieves the list of distributed object names stored in cache.
     * 
     * @return the list of distributed object names stored in cache
     */
    public abstract List<String> getKeys();
}
