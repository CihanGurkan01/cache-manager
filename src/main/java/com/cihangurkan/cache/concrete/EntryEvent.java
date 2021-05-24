package com.cihangurkan.cache.concrete;

import java.io.Serializable;

/**
 * @author Okan ARDIC
 *
 */
public class EntryEvent<K, V> implements Serializable {

    private static final long serialVersionUID = 7489286283352201862L;

    K key;
    V value;
    V oldValue;
    int itemIndex;

    /**
     * @param key
     * @param value
     * @param oldValue
     */
    public EntryEvent(K key, V value) {
        this(key, value, null);
    }

    /**
     * @param key
     * @param value
     * @param oldValue
     */
    public EntryEvent(K key, V value, V oldValue) {
        this(key, value, oldValue, -1);
    }

    /**
     * @param key
     * @param value
     * @param oldValue
     */
    public EntryEvent(K key, V value, V oldValue, int itemIndex) {
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
        this.itemIndex = itemIndex;
    }

    /**
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(K key) {
        this.key = key;
    }

    /**
     * @return the value
     */
    public V getValue() {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue(V value) {
        this.value = value;
    }

    /**
     * @return the oldValue
     */
    public V getOldValue() {
        return oldValue;
    }

    /**
     * @param oldValue the oldValue to set
     */
    public void setOldValue(V oldValue) {
        this.oldValue = oldValue;
    }

    /**
     * @return the itemIndex
     */
    public int getItemIndex() {
        return itemIndex;
    }

    /**
     * @param itemIndex the itemIndex to set
     */
    public void setItemIndex(int itemIndex) {
        this.itemIndex = itemIndex;
    }
}
