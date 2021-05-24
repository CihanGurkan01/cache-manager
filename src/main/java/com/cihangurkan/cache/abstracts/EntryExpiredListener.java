package com.cihangurkan.cache.abstracts;

import com.cihangurkan.cache.concrete.EntryEvent;

/**
 * @author Okan ARDIC
 *
 */
public interface EntryExpiredListener<K, V> extends MapListener {

    /**
     * Invoked when an entry in the distributed map expires.
     * 
     * @param event event object
     */
    void entryExpired(EntryEvent<K, V> event);
}
