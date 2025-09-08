package org.guoguo.broker.ConsumerGroup;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
//@Data
public class ConsumerGroupManager {
    /**
     * 存储所有消费者组：key=组ID，value=消费者组实体
     */

    private final Map<String, ConsumerGroup> groupMap = new ConcurrentHashMap<>();

    //这里返回一个不可修改的视图
    public Map<String, ConsumerGroup> getGroupMap() {
        return Collections.unmodifiableMap(groupMap);
    }

    // return new HashMap<>(groupMap); 深拷贝方法

    //获取创建消费者组 没有就创建
    public ConsumerGroup getOrCreateConsumerGroup(String groupId) {
        //相当于若是不存在就创建的方法
        return groupMap.computeIfAbsent(groupId, k -> {
            ConsumerGroup consumerGroup = new ConsumerGroup();
            consumerGroup.setGroupId(groupId);
            log.info("GuoGuomq=====================>消费者组不存在 创建消费者组：{}", groupId);
            return consumerGroup;
        });
    }

    //消费者订阅主题
    public void GroupSubscribe(SubscribeReqDTO subscribeReqDTO) {
        String topic = subscribeReqDTO.getTopic();
        ConsumerGroup consumerGroup = getOrCreateConsumerGroup(subscribeReqDTO.getGroupId());

        //记录组的订阅关系
        consumerGroup.addSubscribe(subscribeReqDTO);
        log.info("GuoGuomq Broker 消费者组{}订阅主题{}（标签：{}）",
                subscribeReqDTO.getGroupId(), topic, subscribeReqDTO.getTags());
    }

    //消费者加入组
    public void consumerJoinGroup(String groupId, String consumerId, io.netty.channel.Channel channel) {
        ConsumerGroup consumerGroup = getOrCreateConsumerGroup(groupId);
        consumerGroup.addConsumer(consumerId, channel);
    }


    // 消费者组取消订阅主题
    public void groupUnsubscribe(String groupId, String topic) {
        ConsumerGroup group = groupMap.get(groupId);
        if (group == null) {
            log.warn("GuoGuomq Broker 消费者组{}不存在，取消订阅失败", groupId);
            return;
        }
        group.removeSubscribe(topic);
        log.info("GuoGuomq Broker 消费者组{}取消订阅主题{}", groupId, topic);
    }

    //消费者离开组
    public void consumerLeaveGroup(String groupId, String consumerId) {
        ConsumerGroup consumerGroup = groupMap.get(groupId);
        if (consumerGroup == null) {
            return;
        }
        consumerGroup.removeConsumer(consumerId);
        log.info("GuoGuomq Broker 消费者{}离开组{}，当前组内在线消费者数：{}",
                consumerId, groupId, consumerGroup.getOnlineConsumers().size());
        // 组内无消费者时，自动销毁组（可选：也可保留组配置）
        if (consumerGroup.isEmpty()) {
            groupMap.remove(groupId);
            log.info("GuoGuomq Broker 消费者组{}无在线消费者，自动销毁", groupId);
        }
    }

    //获取订阅了指定主题的所有消费者组
    public Map<String, ConsumerGroup> getGroupsByTopic(String topic) {
        Map<String, ConsumerGroup> result = new ConcurrentHashMap<>();
        for (ConsumerGroup group : groupMap.values()) {
            if (group.getSubscribeMap().containsKey(topic)) {
                result.put(group.getGroupId(), group);
            }
        }
        return result;
    }

    // 更新消费者组的消费位点
    public void updateGroupOffset(String groupId, String topic, String messageId) {
        ConsumerGroup consumerGroup = groupMap.get(groupId);
        if (consumerGroup == null) {
            log.warn("GuoGuomq Broker 消费者组{}不存在，无法更新位点", groupId);
            return;
        }
        // 仅更新为更大的消息ID（确保位点递增，避免回退）
        String currentOffset = consumerGroup.getTopicOffsetMap().get(topic);
        if (currentOffset == null || Long.parseLong(messageId) > Long.parseLong(currentOffset)) {
            consumerGroup.getTopicOffsetMap().put(topic, messageId);
            log.info("GuoGuomq Broker 更新组{}的主题{}消费位点至消息{}",
                    groupId, topic, messageId);
        }
    }

}
