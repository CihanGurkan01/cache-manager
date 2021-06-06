package com.cihangurkan.cache.manager;

import java.util.List;

import javax.annotation.PostConstruct;

import com.cihangurkan.common.BeanManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import com.cihangurkan.cache.abstracts.CacheManager;
import com.cihangurkan.cache.common.CacheMethodInfo;
import com.cihangurkan.cache.concrete.CacheEventListener;
import com.cihangurkan.cache.concrete.CacheMainThread;
import com.cihangurkan.cache.util.CacheUtil;

@Component
@EnableScheduling
public class CustomCacheManager {

    private CacheEventListener cacheEventListener;
    private CacheManager cacheManager;
    private BeanManager beanManager;

    private static List<CacheMethodInfo> cacheMethods;

    @Autowired
    public CustomCacheManager(CacheEventListener cacheEventListener,
                              CacheManager cacheManager,
                              BeanManager beanManager) {
        this.cacheEventListener = cacheEventListener;
        this.cacheManager = cacheManager;
        this.beanManager = beanManager;
    }

    @PostConstruct
    public void init() {
        CacheUtil.init(cacheManager, cacheEventListener);
        cacheMethods = beanManager.getAllCacheMethods();
        if (!cacheMethods.isEmpty()) {
            CacheMainThread cacheMainThread = new CacheMainThread(beanManager);
            cacheMainThread.setName("Cahce MainThread");
            cacheMainThread.start();
        }
    }

    public static List<CacheMethodInfo> getAllCacheMethods() {
        return cacheMethods;
    }

}
