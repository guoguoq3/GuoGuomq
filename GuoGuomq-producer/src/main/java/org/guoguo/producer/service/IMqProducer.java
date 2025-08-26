package org.guoguo.producer.service;

import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.producer.pojo.Result;

public interface IMqProducer {
    /** 发送消息 */
    Result<String> send(MqMessage message);
}
