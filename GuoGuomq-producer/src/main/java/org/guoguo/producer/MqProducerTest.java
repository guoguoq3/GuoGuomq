package org.guoguo.producer;

import lombok.extern.slf4j.Slf4j;

import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
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

//        MqMessageEnduring msg1 = new MqMessageEnduring();
//        msg1.setTopic("TEST_TOPIC");
//        msg1.setTags(Arrays.asList("TAG1"));
//        msg1.setPayload("组测试消息1");
//
//        MqMessageEnduring msg2 = new MqMessageEnduring();
//        msg2.setTopic("TEST_TOPIC");
//        msg2.setTags(Arrays.asList("TAG1"));
//        msg2.setPayload("组测试消息2");
//
//        MqMessageEnduring msg3 = new MqMessageEnduring();
//        msg3.setTopic("TEST_TOPIC");
//        msg3.setTags(Arrays.asList("TAG1"));
//        msg3.setPayload("组测试消息3");
//
//        Result<String> send = mqProducer.send(msg1);
//        Result<String> send0 = mqProducer.send(msg3);
//        Result<String> send1 = mqProducer.send(msg2);
//        //message.setEnduring(false);// 是否持久化消息(可选，默认为ture)

        // 2. 发送消息
        for (int i = 0; i < 1000; i++){
            MqMessageEnduring message = new MqMessageEnduring();
            message.setTopic("TEST_TOPIC");
            message.setTags(Arrays.asList("TAG1"));
            message.setPayload("测试消息" + i);
            Result<String> result = mqProducer.send(message);
        }


        // 3. 验证发送结果
        log.info("【生产者测试】发送结果：{}，消息ID：{}");
//        if (re.getCode()==200) {
//            log.info("【生产者测试】消息发送成功");
//        } else {
//            log.error("【生产者测试】消息发送失败");
//        }
    }
}
