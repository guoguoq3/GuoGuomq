package org.guoguo.broker.core;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.VO.PushMessageDTO;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
@Slf4j
public class BrokerManager {
    private final SnowflakeIdGeneratorUtil snowflakeIdGeneratorUtil = new SnowflakeIdGeneratorUtil();
    // 单例模式（简化处理）
    private static final BrokerManager INSTANCE = new BrokerManager();
    public static BrokerManager getInstance() { return INSTANCE; }
    private BrokerManager() {}
    // 订阅关系：主题 -> 消费者通道列表（一个主题可以被多个消费者订阅）同样的这个也支持 一个消费者订阅多个主题
    private final Map<String, List<Channel>> topicChannelMap = new ConcurrentHashMap<>();

    // 存储消息（简化：内存存储，实际应持久化到文件/数据库）todo: 做持久化
    private final Map<String, MqMessage> messageMap = new ConcurrentHashMap<>();

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

    }

    /**
     * 处理生产者发送的消息：存储消息并推送给订阅者
     */
    public void handlerMessage(MqMessage mqMessage){
        String messageId= String.valueOf(snowflakeIdGeneratorUtil.nextId());
        messageMap.put(messageId, mqMessage);
        //做持久化
        log.info("GuoGuomq=====================>存储消息：{}", messageId);

        //将生产者生产的消息推送给订阅者
        String topic = mqMessage.getTopic();
        List<Channel> channelList = topicChannelMap.get(topic);
        if (channelList != null && !channelList.isEmpty()) {
            for (Channel channel : channelList) {
              try {
                  if (channel.isActive()){
                      pushMessageToConsumer(channel, messageId, mqMessage);
                      log.info("GuoGuomq=====================>发送消息：{} - {}", topic, channel.remoteAddress());
                  }
              }catch (Exception e){
                  log.error("GuoGuomq======================>,发送消息失败，通道宕机：{} - {}", topic, channel.remoteAddress());
              }
            }
        }else {
            log.info("GuoGuomq======================>无消费者订阅该主题：{}", topic);
        }
    }

    /**
     * 向消费者推送消息
     */
    private void pushMessageToConsumer(Channel channel, String messageId, MqMessage message) {

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

        String messageStr = JSON.toJSONString(rpcMessageDTO);

        //发送RPC消息
        channel.writeAndFlush(messageStr);
        log.info("GuoGuomq======================>发送RPC消息：{} - {}", message.getTopic(), channel.remoteAddress());




    }







}
