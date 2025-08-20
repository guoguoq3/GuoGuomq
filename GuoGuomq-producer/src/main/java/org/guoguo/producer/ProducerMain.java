package org.guoguo.producer;


import lombok.RequiredArgsConstructor;
import org.guoguo.common.MqMessage;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.Impl.MqProducer;

import java.util.Arrays;
@RequiredArgsConstructor
public class ProducerMain {
    private final MqProducer producer;
    public static void main(String[] args) {
        MqProducer producer = new MqProducer();
        producer.start();

        MqMessage message = new MqMessage();
        message.setTopic("TEST_TOPIC");
        message.setTags(Arrays.asList("TAG1"));
        message.setPayload("Hello, MQ!");

        Result<String> result = producer.send(message);
        System.out.println("发送结果：" + result.getMessage() + "，消息ID：" + result.getMessageId());
    }
}