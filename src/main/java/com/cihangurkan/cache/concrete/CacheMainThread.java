package com.cihangurkan.cache.concrete;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cihangurkan.cache.common.CacheMethodInfo;
import com.cihangurkan.cache.utilities.CacheCommonUtil;
import com.cihangurkan.common.BeanManager;

public class CacheMainThread extends Thread {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private int smallestRefreshTime = 60000;
    private List<CacheMethodInfo> listCacheMethodInfos = null;
    private BeanManager beanManager;

    public CacheMainThread(BeanManager beanManager) {
        this.beanManager = beanManager;
        listCacheMethodInfos = beanManager.getAllCacheMethods();
        for (CacheMethodInfo cacheMethodInfo : listCacheMethodInfos) {
            if ((cacheMethodInfo.getRefreshPeriod() * 1000) < smallestRefreshTime)
                smallestRefreshTime = cacheMethodInfo.getRefreshPeriod() * 1000;
        }
        logger.info("Cache Main Thread initialized and executing period is {} ms.",
                    smallestRefreshTime);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread thread = null;
                for (CacheMethodInfo cacheMethodInfo : listCacheMethodInfos) {
                    thread = new Thread(new MethodInvokerThread(cacheMethodInfo));
                    thread.setName(CacheCommonUtil.getKeyAsMethodName(cacheMethodInfo.getMethod())
                                   + " Thread");
                    thread.start();
                }

                Thread.sleep(smallestRefreshTime);
            } catch (Exception e) {
                logger.error("Exception occurred while invoking methods");
            }
        }
    }

}
