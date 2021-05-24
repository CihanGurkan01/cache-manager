package com.cihangurkan.cache.configuration;

import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.cihangurkan.cache.redis.concrete.RedissonCacheManager;
import com.cihangurkan.cache.redis.concrete.RedissonProperties;

@Configuration
@ConditionalOnClass(Config.class)
@EnableConfigurationProperties(RedissonProperties.class)
@ConditionalOnProperty(name = "spring.cache.type", havingValue = "redis")
public class RedissonConfiguration {

    @Autowired
    private RedissonProperties redissonProperties;

    @Bean
    @ConditionalOnProperty(name = "redisson.address")
    public RedissonClient redissonSingle() {
        Config config = new Config();

        SingleServerConfig serverConfig = config.useSingleServer()
                        .setAddress(redissonProperties.getAddress())
                        .setTimeout(redissonProperties.getTimeout())
                        .setConnectionPoolSize(redissonProperties.getConnectionPoolSize())
                        .setConnectionMinimumIdleSize(redissonProperties
                                        .getConnectionMinimumIdleSize());
        if (StringUtils.isNotBlank(redissonProperties.getPassword())) {
            serverConfig.setPassword(redissonProperties.getPassword());
        }
        return Redisson.create(config);
    }

    @Bean
    public RedissonCacheManager redisCacheUtil(RedissonClient redissonClient) {
        RedisClient nativeClient = ((org.redisson.Redisson) redissonClient).getConnectionManager()
                        .getEntrySet().iterator().next().getClient();

        RedissonCacheManager cacheManager = new RedissonCacheManager(redissonClient,
                        nativeClient.connect(), redissonProperties);

        return cacheManager;
    }

    //    /**
    //     * Retrieves a connection for Redis to execute native Redis commands which are not supported by
    //     * Redisson.
    //     * 
    //     * @return
    //     */
    //    @Bean
    //    RedisConnection nativeConnection() {
    //        // Use shared EventLoopGroup only if multiple clients are used
    //        EventLoopGroup group = new NioEventLoopGroup();
    //
    //        RedisClientConfig config = new RedisClientConfig();
    //        config.setAddress(redissonProperties.getAddress()) // or rediss:// for ssl connection
    //                        .setDatabase(0).setClientName("nativeClient").setGroup(group);
    //
    //        if (StringUtils.isNotBlank(redissonProperties.getPassword())) {
    //            config.setPassword(redissonProperties.getPassword());
    //        }
    //
    //        RedisClient client = RedisClient.create(config);
    //        return client.connect();
    //    }
    //
    //    @Bean
    //    //    @ConditionalOnProperty(name = "spring.cache.type", havingValue = "redis")
    //    RedissonCacheManager redisCacheUtil(RedissonClient redissonClient,
    //                                        RedisConnection nativeConnection) {
    //        RedissonCacheManager cacheManager = new RedissonCacheManager(redissonClient,
    //                        nativeConnection);
    //
    //        cacheManager.getMemberCount();
    //        return cacheManager;
    //    }

}
