package com.cihangurkan.cache.utilities;

import java.lang.reflect.Method;

public class CacheCommonUtil {

    public static String getKeyAsMethodName(Method method) {
        String key = method.getDeclaringClass().getName();
        if (key.contains("$"))
            key = key.split("\\$")[0];

        return key + "." + method.getName();
    }
}
