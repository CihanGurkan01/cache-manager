package com.cihangurkan.cache.concrete;

import com.cihangurkan.cache.abstracts.EntryExpiredListener;

/**
 * Configuration class for distributed objects. It supports setting time-to-live value and maximum
 * item size for distributed objects.
 * 
 * @author Okan ARDIC
 *
 */
public class DistItemConfig<K, V> {

    private int timeToLiveSeconds;
    private int maxSize = 0;
    private EntryExpiredListener<K, V> entryExpiredListener;

    /**
     * @return time-to-live in seconds the distributed object will keep the items, if timeout
     *         expires items will be automatically evicted. 0 means no timeout.
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * @param timeToLiveSeconds the timeToLiveSeconds to set
     */
    public void setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
    }

    /**
     * @return maximum size the distributed object can store. 0 means no limit.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * @param maxSize the maxSize to set
     */
    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * @return the entryExpiredListener
     */
    public EntryExpiredListener<K, V> getEntryExpiredListener() {
        return entryExpiredListener;
    }

    /**
     * @param entryExpiredListener the entryExpiredListener to set
     */
    public void setEntryExpiredListener(EntryExpiredListener<K, V> entryExpiredListener) {
        this.entryExpiredListener = entryExpiredListener;
    }
}
