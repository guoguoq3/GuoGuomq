package org.guoguo.common.pojo.VO;


import lombok.Data;
import org.guoguo.common.pojo.Entity.MqMessage;

/**
 * 推送给消费者的消息
 */
@Data
public class PushMessageDTO {
    /** 消息ID（Broker生成，用于确认） */
    private String messageId;
    /** 原始消息内容 */
    private MqMessage message;
    /** 消费者组 */
    private String consumerGroup;
}