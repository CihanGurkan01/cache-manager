package com.cihangurkan.cache.concrete;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cihangurkan.cache.common.CacheMethodInfo;
import com.cihangurkan.cache.entities.CacheEntity;
import com.cihangurkan.cache.util.CacheUtil;
import com.cihangurkan.cache.utilities.CacheCommonUtil;

public class MethodInvokerThread implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private CacheMethodInfo cacheMethodInfo;
    private Calendar calendar;

    public MethodInvokerThread(CacheMethodInfo cacheMethodInfo) {
        this.cacheMethodInfo = cacheMethodInfo;
        calendar = Calendar.getInstance();
    }

    public Object invoke() throws IllegalAccessException,
                           IllegalArgumentException,
                           InvocationTargetException {
        return cacheMethodInfo.getMethod().invoke(cacheMethodInfo.getObject());

    }

    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            CacheEntity cacheEntity = CacheUtil.getRequestByRef(CacheCommonUtil
                            .getKeyAsMethodName(cacheMethodInfo.getMethod()));
            if (cacheEntity == null) {
                putCacheEntityToMap();
                logger.debug("İnitial put to redis cache map for method {} finished in {} ms",
                             CacheCommonUtil.getKeyAsMethodName(cacheMethodInfo.getMethod()),
                             (System.currentTimeMillis() - start));
            } else {
                if ((cacheEntity.getDate().before(new Date()) && cacheEntity.getUuidLock() == null)
                    || checkTimeout(cacheEntity, cacheMethodInfo)) {
                    lockEntity(cacheEntity);
                    putCacheEntityToMap();
                    logger.debug("Updating redis cache map for method {} finished in {} ms",
                                 CacheCommonUtil.getKeyAsMethodName(cacheMethodInfo.getMethod()),
                                 (System.currentTimeMillis() - start));
                }
            }

        } catch (Exception e) {
            logger.error("Exception occurred while executing cache method : {}.",
                         CacheCommonUtil.getKeyAsMethodName(cacheMethodInfo.getMethod()), e);
        }
    }

    private boolean checkTimeout(CacheEntity cacheEntity, CacheMethodInfo cacheMethodInfo) {
        calendar.setTime(cacheEntity.getDate());
        calendar.add(Calendar.SECOND, cacheMethodInfo.getRefreshPeriod());
        return calendar.getTime().before(new Date());
    }

    private Date getNextRefreshDate() {
        return new Date(System.currentTimeMillis() + cacheMethodInfo.getRefreshPeriod() * 1000);
    }

    private void lockEntity(CacheEntity cacheEntity) {
        cacheEntity.setUuidLock(UUID.randomUUID());
        CacheUtil.storeRequestByRef(CacheCommonUtil.getKeyAsMethodName(cacheMethodInfo.getMethod()),
                                    cacheEntity);
    }

    private void putCacheEntityToMap() throws IllegalAccessException,
                                       IllegalArgumentException,
                                       InvocationTargetException {
        Serializable object = (Serializable) invoke();
        if (object != null) {
            CacheEntity cacheEntity = new CacheEntity(object, getNextRefreshDate(), null);
            CacheUtil.storeRequestByRef(CacheCommonUtil
                            .getKeyAsMethodName(cacheMethodInfo.getMethod()), cacheEntity);
        }

    }

}
