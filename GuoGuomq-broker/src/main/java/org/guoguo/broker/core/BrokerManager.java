package org.guoguo.broker.core;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.util.FilePersistUtil;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
@Slf4j
@Component
public class BrokerManager {
    private final SnowflakeIdGeneratorUtil snowflakeIdGeneratorUtil = new SnowflakeIdGeneratorUtil();

    // 单例模式（简化处理） 现转为spring管理
    //  private static final BrokerManager INSTANCE = new BrokerManager();
    //public static BrokerManager getInstance() { return INSTANCE; }
    // private BrokerManager() {}

    // 订阅关系：主题 -> 消费者通道列表（一个主题可以被多个消费者订阅）同样的这个也支持 一个消费者订阅多个主题
    private final Map<String, List<Channel>> topicChannelMap = new ConcurrentHashMap<>();

    //提供 messageMap 的 getter（供 FilePersistUtil 恢复消息）
    // 存储消息（简化：内存存储，实际应持久化到文件/数据库）
    @Getter
    private final Map<String, MqMessageEnduring> messageMap = new ConcurrentHashMap<>();

    // 3. 消费状态存储：key=消息ID+":"+消费者ID，value=消费状态（SUCCESS/FAIL）
    private final Map<String, String> messageConsumeMap = new ConcurrentHashMap<>();

    private final FilePersistUtil filePersistUtil;
    @Autowired
    public BrokerManager(@Lazy  FilePersistUtil filePersistUtil) {
        this.filePersistUtil = filePersistUtil;
    }



    /**
     * 处理订阅请求：记录主题与消费者通道的关系
     */
    public void handlerSubscribe(SubscribeReqDTO subscribeReqDTO, Channel channel){
        String topic = subscribeReqDTO.getTopic();
        //如果没创建过
        topicChannelMap.computeIfAbsent(topic, k -> new ArrayList<>());

        List<Channel> channelList = topicChannelMap.get(topic);
        if (!channelList.contains(channel)){
            channelList.add(channel);
            log.info("GuoGuomq=====================>订阅关系：{} - {}", topic, channel.remoteAddress());
        }

        // 2. 新增：回溯该主题的历史消息，推送给新订阅的消费者
//        backtrackHistoricalMessages(topic, channel);

    }

    /**
     * 回溯历史消息：将该主题下的未消费消息推送给消费者
     */
    public void backtrackHistoricalMessages(String topic, Channel channel) {
        String consumerId = channel.id().asLongText(); // 消费者唯一标识
        log.info("GuoGuomq Broker 开始为消费者{}回溯主题{}的历史消息", consumerId, topic);

        // 遍历所有消息，筛选出该主题的消息
        for (Map.Entry<String, MqMessageEnduring> entry : messageMap.entrySet()) {
            String messageId = entry.getKey();
            MqMessageEnduring message = entry.getValue();

            // 只处理当前主题的消息
            if (!topic.equals(message.getTopic())) {
                continue;
            }

            // 检查该消费者是否已确认过此消息
            String consumeKey = messageId + ":" + consumerId;
            if (messageConsumeMap.containsKey(consumeKey)) {
                log.info("GuoGuomq Broker 消费者{}已确认消息{}，跳过回溯", consumerId, messageId);
                continue;
            }

            // 未确认：推送历史消息给消费者
            try {
                if (channel.isActive()) {
                    pushSingleConsumer(channel, messageId, message);
                    log.info("GuoGuomq Broker 向消费者{}回溯消息{}", consumerId, messageId);
                }
            } catch (Exception e) {
                log.error("GuoGuomq Broker 回溯消息{}给消费者{}失败", messageId, consumerId, e);
            }
        }

        log.info("GuoGuomq Broker 主题{}历史消息回溯完成", topic);
    }
    /**
     * 处理生产者发送的消息：存储消息并推送给订阅者
     */
    public void handlerMessage(MqMessageEnduring mqMessage, String messageId){
        messageMap.put(messageId, mqMessage);
        if(mqMessage.isEnduring()){//默认为true，即进行持久化
            //做持久化
            log.info("GuoGuomq=====================>存储消息：{}", messageId);

            //核心持久化时机是在 Broker 接收到生产者的消息后、尚未发送给消费者之前完成存储
            filePersistUtil.writeMessage(messageId, mqMessage);
        }

        //将生产者生产的消息推送给订阅者
        String topic = mqMessage.getTopic();
        //推送前检查ack状态
        pushMessageToConsumers(messageId, mqMessage);
    }

    /**
     * 向消费者推送消息
     */
    private void pushSingleConsumer(Channel channel, String messageId, MqMessageEnduring message) {

        //构建发送给消费者的消息
        PushMessageDTO pushMsg = new PushMessageDTO();
        pushMsg.setMessageId(messageId);
        pushMsg.setMessage(message);

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

    /*
    推送消息给订阅主题的消费者
     */
    private void pushMessageToConsumers(String messageId, MqMessageEnduring message) {
         String topic=message.getTopic();
         List<Channel> channelList = topicChannelMap.get(topic);
        if (channelList == null || channelList.isEmpty()) {
            log.info("GuoGuomq Broker 无消费者订阅主题：{}，消息ID={}", topic, messageId);
            return;
        }

        for (Channel channel : channelList){
            try {
                if (!channel.isActive()) {
                    log.warn("GuoGuomq Broker 消费者通道已断开：{}，跳过推送", channel.remoteAddress());
                    continue;
                }

                //生成消费者id 通道用一个 注意这个是消费者id加消息id故而是面向于每一个消费者的
                ChannelId channelId = channel.id();
                String consumerId = channelId.asLongText();
                //生成消费状态的key
                String consumerKey = channelId + ":" + consumerId;

                //若是此条消息已经被确认过了
                if (messageConsumeMap.containsKey(consumerKey)){
                    String status=messageConsumeMap.get(consumerKey);
                    log.info("GuoGuomq Broker 消费者{}已确认消息{}（状态：{}），跳过推送",
                            consumerId, messageId, status);
                    continue;
                }
                //未推送过这条消息
               pushSingleConsumer(channel, messageId, message);
            }catch (Exception e){
                log.error("GuoGuomq Broker 推送消息给消费者失败：{} - {}", topic, channel.remoteAddress());
            }
        }


    }
    /**
     * 处理消费者的 ACK 请求：更新消费状态
     */
    public void handlerConsumerAck(ConsumerAckReqDTO consumerAckReqDTO,Channel  channel){
        String messageId=consumerAckReqDTO.getMessageId();
        String consumerId=consumerAckReqDTO.getConsumerId();
        String ackStatus=consumerAckReqDTO.getAckStatus();

        //校验
        if (messageId==null || consumerId==null){
            log.error("GuoGuomq Broker 消费确认请求参数无效：messageId={}，consumerId={}", messageId, consumerId);
            return;
        }

        // 2. 检查消息是否存在（避免确认不存在的消息）
        if (!messageMap.containsKey(messageId)) {
            log.error("GuoGuomq Broker 消费确认的消息{}不存在", messageId);
            return;
        }

        // 3. 更新消费状态的key
        String consumerKey = channel.id() + ":" + consumerId;
        messageConsumeMap.put(consumerKey, ackStatus);
        log.info("GuoGuomq Broker 消费者{}确认消息{}（状态：{}）", consumerId, messageId, ackStatus);

//        //向消费者返回ack成功的响应
//        RpcMessageDTO response = new RpcMessageDTO();
//        response.setTraceId(String.valueOf(snowflakeIdGeneratorUtil.nextId()));
//        response.setRequest(false);
//        response.setMethodType(MethodType.C_ACK_MSG);
//        response.setJson("SUCCESS");
//        channel.writeAndFlush(JSON.toJSONString(response));



    }






}
