package org.guoguo.consumer.test;

import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.impl.MqConsumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest(classes = org.guoguo.consumer.ConsumerApplication.class)
@TestPropertySource(locations = "classpath:application.properties")
public class MqConsumerAckTest {

    @Autowired
    private MqConsumer mqConsumer;

    // 用于阻塞测试，等待消息处理和 ACK 完成
    private final CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void testConsumeWithAck() throws InterruptedException {
        // 1. 构造订阅请求（订阅 TEST_TOPIC 主题，TAG1 标签）
        SubscribeReqDTO subscribeReq = new SubscribeReqDTO();
        subscribeReq.setTopic("TEST_TOPIC");
        subscribeReq.setTags(Arrays.asList("TAG1"));

        // 2. 自定义监听器：处理消息并返回“处理成功”
        IMessageListener listener = new IMessageListener() {
            @Override
            public boolean onMessage(MqMessage message) {
                // 模拟业务处理（如存储数据库、调用接口等）
                log.info("【ACK测试】消费者处理消息：内容={}，标签={}",
                        message.getPayload(), message.getTags());
                
                // 处理成功，返回 true（触发 ACK 发送）
                return true;
            }
        };

        // 3. 订阅主题并绑定监听器
        mqConsumer.subscribe(subscribeReq, listener);

        // 4. 阻塞等待（等待消息接收、处理、发送 ACK）
        latch.await(15, TimeUnit.MINUTES);
        log.info("【ACK测试】测试结束");
    }
}