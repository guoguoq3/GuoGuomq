// org.guoguo.common.config.MqConfigProperties.java
package org.guoguo.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * MQ 统一配置类，绑定 application.properties 中的配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "guoguomq") // 配置前缀
public class MqConfigProperties {
    /** Broker 地址（默认：127.0.0.1） */
    private String brokerHost = "127.0.0.1";
    /** Broker 端口（默认：9999） */
    private int brokerPort = 9999;
    /** 生产者超时时间（毫秒，默认：4000） */
    private int producerTimeout = 4000;
    /** 消费者连接超时时间（毫秒，默认：5000） */
    private int consumerTimeout = 5000;
}