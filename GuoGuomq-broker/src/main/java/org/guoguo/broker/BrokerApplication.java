// org.guoguo.broker.BrokerApplication.java
package org.guoguo.broker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.guoguo.common.config.MqConfigProperties;

@SpringBootApplication
@EnableConfigurationProperties(MqConfigProperties.class) // 启用配置绑定
public class BrokerApplication {
    public static void main(String[] args) {
        SpringApplication.run(BrokerApplication.class, args);
    }
}