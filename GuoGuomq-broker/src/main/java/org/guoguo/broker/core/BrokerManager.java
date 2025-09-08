package org.guoguo.broker.core;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.ConsumerGroup.ConsumerGroupManager;
import org.guoguo.broker.util.FilePersistUtil;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.Entity.ConsumerGroup;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.guoguo.common.pojo.VO.PushMessageDTO;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
@Slf4j
@Component
public class BrokerManager {
    private final SnowflakeIdGeneratorUtil snowflakeIdGeneratorUtil = new SnowflakeIdGeneratorUtil();
    // 注入消费者组管理器
    private final ConsumerGroupManager groupManager;

    // 订阅关系：主题 -> 消费者通道列表（一个主题可以被多个消费者订阅）同样的这个也支持 一个消费者订阅多个主题
    private final Map<String, List<Channel>> topicChannelMap = new ConcurrentHashMap<>();

    //提供 messageMap 的 getter（供 FilePersistUtil 恢复消息）
    // 存储消息（简化：内存存储，实际应持久化到文件/数据库）todo: 做持久化
    @Getter
    private final Map<String, MqMessage> messageMap = new ConcurrentHashMap<>();

    // 单个消费者的确认状态（避免同组内重复推送）：key=消息ID+消费者ID，value=状态
    private final Map<String, String> consumerAckMap = new ConcurrentHashMap<>();

    // 3. 消费状态存储：key=消息ID+":"+消费者ID，value=消费状态（SUCCESS/FAIL）
    private final Map<String, String> messageConsumeMap = new ConcurrentHashMap<>();
    //消费位点存储（key=topic:consumerGroup，value=最新消费的消息ID）
    private final Map<String, String> consumeOffsetMap = new ConcurrentHashMap<>();

    private final FilePersistUtil filePersistUtil;
    @Autowired
    public BrokerManager(@Lazy  FilePersistUtil filePersistUtil,ConsumerGroupManager consumerGroupManager) {
        this.filePersistUtil = filePersistUtil;
        this.groupManager = consumerGroupManager;
    }



    /**
     * 处理订阅请求：记录主题与消费者通道的关系
     */
//    public void handlerSubscribe(SubscribeReqDTO subscribeReqDTO, Channel channel){
//        String topic = subscribeReqDTO.getTopic();
//        String consumerGroup=subscribeReqDTO.getTopic();
//        if (consumerGroup== null){
//            throw new  IllegalArgumentException("消费分组（consumerGroup）不能为空");
//
//        }
//
//        // 记录订阅关系 按照消费者分组和主题来记录
//        String groupKey = topic + ":" + consumerGroup;
//
//        //如果没创建过
//        topicChannelMap.computeIfAbsent(groupKey, k -> new ArrayList<>());
//        List<Channel> channelList = topicChannelMap.get(groupKey);
//        if (!channelList.contains(channel)){
//            channelList.add(channel);
//            log.info("Broker 记录订阅关系：主题={}，分组={}，通道={}", topic, consumerGroup, channel.id());
//        }
//
//        // 新增：回溯该主题的历史消息，推送给新订阅的消费者
//       backtrackHistoricalMessages(topic,consumerGroup,channel);
//
//    }

    /**
     * 回溯历史消息：只推送消费位点之后的未消费消息
     */
//    public void backtrackHistoricalMessages(String topic,String consumerGroup, Channel channel) {
//        String groupKey = topic + ":" + consumerGroup;
//        String consumerId = channel.id().asLongText(); // 消费者唯一标识
//        log.info("为分组{}的消费者{}回溯主题{}的消息", consumerGroup, consumerId, topic);
//
//        // 获取该消费者消费位点
//        String lastConsumerOffset=consumeOffsetMap.get(groupKey);
//
//        // 遍历所有消息，筛选出 是同一主题并在消费者位点之后的消息
//        List<Map.Entry<String,MqMessage>> messageList=new ArrayList<>(messageMap.entrySet());
//        // 按消息ID排序（消息ID是递增的，如雪花ID） todo:这里要改
//        messageList.sort(Comparator.comparing(Map.Entry::getKey));
//
//
//        for (Map.Entry<String, MqMessage> entry : messageList) {
//            String messageId = entry.getKey();
//            MqMessage message = entry.getValue();
//
//            // 只处理当前主题的消息
//            if (!topic.equals(message.getTopic())) {
//                continue;
//            }
//
//            // 检查该消费者是否已确认过此消息
//            String consumeKey = messageId + ":" + consumerId;
//            if (messageConsumeMap.containsKey(consumeKey)) {
//                log.info("GuoGuomq Broker 消费者{}已确认消息{}，跳过回溯", consumerId, messageId);
//                continue;
//            }
//
//            // 未确认：推送历史消息给消费者
//            try {
//                if (channel.isActive()) {
//                    pushSingleConsumer(channel, messageId, message, consumerGroup);
//                    log.info("GuoGuomq Broker 向消费者{}回溯消息{}", consumerId, messageId);
//                }
//            } catch (Exception e) {
//                log.error("GuoGuomq Broker 回溯消息{}给消费者{}失败", messageId, consumerId, e);
//            }
//        }
//
//        log.info("GuoGuomq Broker 主题{}历史消息回溯完成", topic);
//    }
    /**
     * 处理生产者发送的消息：存储消息并推送给订阅者
     */
    public void handlerMessage(MqMessageEnduring mqMessage, String messageId){

        messageMap.put(messageId, mqMessage);

        //核心持久化时机是在 Broker 接收到生产者的消息后、尚未发送给消费者之前完成存储 todo：生产者做持久化标识
        filePersistUtil.writeMessage(messageId, mqMessage);
        //做持久化
        log.info("GuoGuomq Broker 存储消息：ID={}，主题={}", messageId, mqMessage.getTopic());

        //将生产者生产的消息推送给订阅者
        String topic = mqMessage.getTopic();
        Map<String, ConsumerGroup> subscribeGroups = groupManager.getGroupsByTopic(topic);
        if (subscribeGroups.isEmpty()) {
            log.info("GuoGuomq Broker 无消费者组订阅主题{}，消息{}暂不推送", topic, messageId);
            return;
        }
        //推送前检查ack状态
        for (ConsumerGroup group : subscribeGroups.values()) {
            pushMessageToGroup(group, messageId, mqMessage);
        }
    }

    /**
     * 向消费者推送消息
     */
    private void pushSingleConsumer(Channel channel, String messageId, MqMessage message,String consumerGroup) {

        //构建发送给消费者的消息
        PushMessageDTO pushMsg = new PushMessageDTO();
        pushMsg.setMessageId(messageId);
        pushMsg.setMessage(message);
        pushMsg.setConsumerGroup(consumerGroup);

        //包装为RPC消息
        RpcMessageDTO rpcMessageDTO = new RpcMessageDTO();
        rpcMessageDTO.setTraceId(String.valueOf(snowflakeIdGeneratorUtil.nextId()));
        rpcMessageDTO.setRequest(false);
        rpcMessageDTO.setMethodType(MethodType.B_PUSH_MSG);
        rpcMessageDTO.setJson(JSON.toJSONString(pushMsg));

        String messageStr = JSON.toJSONString(rpcMessageDTO) + "\n";

        //发送RPC消息
        channel.writeAndFlush(messageStr);
        log.info("GuoGuomq======================>发送RPC消息：{} - {}", message.getTopic(), channel.remoteAddress());


    }

    /**
     * 向消费者组推送消息（组内负载均衡：轮询分配给在线消费者）
     */
    private void pushMessageToGroup(ConsumerGroup group, String messageId, MqMessage message) {
        String groupId = group.getGroupId();
        String topic = message.getTopic();
        Map<String, Channel> onlineConsumers = group.getOnlineConsumers();

        if (onlineConsumers.isEmpty()){
            log.info("GuoGuomq Broker 无消费者组订阅主题{}，消息{}暂不推送", topic, messageId);
            return;
        }

        // 检查组的消费位点：只推送位点之后的消息
        String groupOffset = group.getTopicOffsetMap().get(topic);
        if (groupOffset != null && Long.parseLong(messageId) <= Long.parseLong(groupOffset)) {
            log.info("GuoGuomq Broker 消息{}已在组{}的消费位点之前，跳过推送", messageId, groupId);
            return;
        }

        //组内负载均衡：轮询选择一个消费者（简化实现）
        List<String> consumerIds = new ArrayList<>(onlineConsumers.keySet());
        // 按消息ID取模，确保同消息分配给同消费者
        int index = (int) (Long.parseLong(messageId) % consumerIds.size());
        String targetConsumerId = consumerIds.get(index);
        Channel targetChannel = onlineConsumers.get(targetConsumerId);

        //检查该消费者是否已确认过此消息（避免重复推送）
        String ackKey=messageId+":"+targetConsumerId;
        if (consumerAckMap.containsKey(ackKey)) {
            log.info("GuoGuomq Broker 消费者{}已确认消息{}，跳过推送", targetConsumerId, messageId);
            return;
        }
        // 4. 推送消息给目标消费者
        try {
            if (targetChannel.isActive()) {
                PushMessageDTO pushDto = new PushMessageDTO();
                pushDto.setMessageId(messageId);
                pushDto.setMessage(message);

                RpcMessageDTO rpcDto = new RpcMessageDTO();
                rpcDto.setRequest(false);
                rpcDto.setTraceId(String.valueOf(snowflakeIdGeneratorUtil.nextId()));
                rpcDto.setMethodType(MethodType.B_PUSH_MSG);
                rpcDto.setJson(JSON.toJSONString(pushDto));
                System.out.println(onlineConsumers);
                targetChannel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n"); // 带分隔符
                log.info("GuoGuomq Broker 向组{}的消费者{}推送消息{}", groupId, targetConsumerId, messageId);
            }
        } catch (Exception e) {
            log.error("GuoGuomq Broker 推送消息{}给组{}的消费者{}失败", messageId, groupId, targetConsumerId, e);
            // todo:失败时可重试分配给其他消费者
        }


    }

    /**
     * 处理消费者的 ACK 更新消费状态
     */
    public void handleConsumerAck(ConsumerAckReqDTO ackReq, String consumerId) {
        String messageId=ackReq.getMessageId();
        String consumerGroup = ackReq.getGroupId();

        //校验
        if (messageId==null || consumerId==null){
            log.error("GuoGuomq Broker 消费确认请求参数无效：messageId={}，consumerId={}", messageId, consumerId);
            return;
        }

        MqMessage message = messageMap.get(messageId);
        if (message == null) {
            log.error("GuoGuomq Broker 消息{}不存在，ACK处理失败", messageId);
            return;
        }

        // 从消息中获取主题
        String topic = messageMap.get(messageId).getTopic();
        String groupKey = topic + ":" + consumerGroup;

        consumerAckMap.put(groupKey, ackReq.getAckStatus());
        log.info("GuoGuomq Broker 收到消费者{}的ACK：消息{}，状态{}", consumerId, messageId, ackReq.getAckStatus());

    }



}
