package org.guoguo.consumer.service;

import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

/**
 * 消费者接口
 */
public interface IMqConsumer {
    /**
     * 订阅主题
     * @param subscribeReqDTO 订阅请求
     * @param listener 消息监听器（处理收到的消息）
     */
    void subscribe(SubscribeReqDTO subscribeReqDTO, IMessageListener listener);

    /**
     * 启动消费者（连接Broker）
     */
    void start();

    /**
     * 关闭消费者
     */
    void close();
}