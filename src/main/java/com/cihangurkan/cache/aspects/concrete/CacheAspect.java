package com.cihangurkan.cache.aspects.concrete;

import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.cihangurkan.cache.aspects.annotation.Cache;
import com.cihangurkan.cache.concrete.MethodInvokerThread;
import com.cihangurkan.cache.entities.CacheEntity;
import com.cihangurkan.cache.util.CacheUtil;
import com.cihangurkan.cache.utilities.CacheCommonUtil;
import com.cihangurkan.security.utilities.results.IResult;

@Aspect
@Component
public class CacheAspect {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Around("@annotation(com.cihangurkan.cache.aspects.annotation.Cache)")
    public Object enterCache(ProceedingJoinPoint joinPoint) throws Throwable {

        Cache cache = getCache(joinPoint);
        String key = getMethodNameAsKey(joinPoint);

        Object object = null;

        if (requestComeFromRefreshThread()) {
            //refresh
            object = joinPoint.proceed();
            if (object != null && object instanceof IResult) {
                if (!((IResult) object).success()) {
                    logger.error("Cache refresh method invoke return null. Redis will not be updated by methodInvokerThread. Method {}",
                                 key);
                    return null;
                }
            }
        } else {
            CacheEntity cacheEntity = CacheUtil.getRequestByRef(key);
            object = cacheEntity.getObject();
        }

        return object;
    }

    public Cache getCache(ProceedingJoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();

        return method.getAnnotation(Cache.class);
    }

    public String getMethodNameAsKey(ProceedingJoinPoint joinPoint) {
        return CacheCommonUtil.getKeyAsMethodName(((MethodSignature) joinPoint.getSignature())
                        .getMethod());
    }

    public boolean requestComeFromRefreshThread() {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stackTraceElements) {
            if (stackTraceElement.getClassName().equals(MethodInvokerThread.class.getName()))
                return true;
        }
        return false;
    }

}
