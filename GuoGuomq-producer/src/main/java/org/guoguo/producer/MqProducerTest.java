package org.guoguo.producer;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.IMqProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;

/**
 * 生产者测试：发送消息并验证结果
 */
@Slf4j
@SpringBootTest(classes = org.guoguo.producer.ProducerApplication.class) // 指定生产者启动类
@TestPropertySource(locations = "classpath:application.properties") // 加载配置
public class MqProducerTest {

    @Autowired
    private IMqProducer mqProducer; // 注入生产者实例

    @Test
    public void testSendMessage() {
        // 1. 构建消息
        MqMessage message = new MqMessage();
        message.setTopic("TEST_TOPIC"); // 主题需与消费者订阅的一致
        message.setTags(Arrays.asList("TAG1")); // 标签需匹配消费者订阅的标签
        message.setPayload("Hello, GuoGuomq43243242342!"); // 消息内容
        message.setBizKey("TEST_BIZ_KEY"); // 业务标识（可选）

        // 2. 发送消息
        Result<String> result = mqProducer.send(message);

        // 3. 验证发送结果
        log.info("【生产者测试】发送结果：{}，消息ID：{}", result.getMessageId(), result.getData());
        if (result.getCode()==200) {
            log.info("【生产者测试】消息发送成功");
        } else {
            log.error("【生产者测试】消息发送失败");
        }
    }
}
