package com.cihangurkan.cache.concrete;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.cihangurkan.cache.abstracts.EntryExpiredListener;

/**
 * Listener class to handle call based events.
 * 
 * @author Okan ARDIC
 *
 */
@Component
public class CacheEventListener implements EntryExpiredListener<Object, Object> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void entryExpired(EntryEvent<Object, Object> event) {
        System.out.println(event);
    }

}
