package com.cihangurkan.cache.configuration;

import com.cihangurkan.common.BeanManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.cihangurkan.cache.hazelcast.concrete.HazelcastCacheManager;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

@Configuration
@ConditionalOnClass(Config.class)
@ConditionalOnProperty(name = "spring.cache.type", havingValue = "hazelcast")
public class HazelcastConfiguration {

	@Autowired
	ApplicationContext applicationContext;
	
    @Autowired
    private BeanManager beanManager;

    @Bean
    HazelcastInstance hazelcastInstance() {
        return Hazelcast.newHazelcastInstance();
    }

    @Bean
    HazelcastCacheManager hazelcastCacheUtil(HazelcastInstance hazelcastInstance) {
        HazelcastCacheManager hazelcastCacheUtil = new HazelcastCacheManager(
        		applicationContext);
        hazelcastCacheUtil.setHazelcastInstance(hazelcastInstance);
        return hazelcastCacheUtil;
    }
}