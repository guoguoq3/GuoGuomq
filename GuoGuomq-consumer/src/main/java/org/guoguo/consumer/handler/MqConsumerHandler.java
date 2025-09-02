package org.guoguo.consumer.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.VO.PushMessageDTO;
import org.guoguo.consumer.service.impl.MqConsumer;

@Slf4j
public class MqConsumerHandler extends SimpleChannelInboundHandler<String> {
    private final MqConsumer consumer;

    public MqConsumerHandler(MqConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        // 解析 Broker 发送的消息
        RpcMessageDTO rpcDto = JSON.parseObject(msg, RpcMessageDTO.class);
        log.info("消费者收到消息：{}", rpcDto);

        // 如果是 Broker 推送的消息
        // 在 MqConsumerHandler 的 channelRead0 方法中
        if (MethodType.B_PUSH_MSG.equals(rpcDto.getMethodType())) {
            // 处理消息推送（原逻辑不变）
            PushMessageDTO pushMsg = JSON.parseObject(rpcDto.getJson(), PushMessageDTO.class);
            String messageId = pushMsg.getMessageId();
            MqMessage message = pushMsg.getMessage();
            String topic = message.getTopic();
            // 1. 调用消费者的监听器处理消息
            log.info("GuoGuomq 消费者处理消息：消息ID={}，主题={}，内容={}",
                    messageId, topic, message.getPayload());
            boolean handleSuccess = consumer.handlerMessage(topic, message); // 调用监听器，返回处理结果
            // 2. 处理成功则发送 ACK 给 Broker
            if (handleSuccess) {
                sendAckToBroker(ctx.channel(), messageId);
            } else {
                log.warn("GuoGuomq 消费者处理消息 {} 失败，不发送 ACK（等待重试）", messageId);
            }


        } else if (MethodType.B_SUBSCRIBE_RESPONSE.equals(rpcDto.getMethodType())) {
            // 处理订阅响应
            log.info("订阅结果：{}", rpcDto.getJson());
        } else {
            log.warn("消费者收到未知类型的消息：{}", rpcDto.getMethodType());
        }
    }

    /**
     * 向 Broker 发送消费确认（ACK）
     *
     * @param channel   消费者与 Broker 的连接通道
     * @param messageId 要确认的消息 ID
     */
    private void sendAckToBroker(Channel channel, String messageId) {
        if (!channel.isActive()) {
            log.error("GuoGuomq 消费者通道已断开，无法发送 ACK：消息 ID={}", messageId);
            return;
        }
// 1. 生成消费者唯一标识（用通道 ID，确保同一消费者的唯一性）
        ChannelId channelId = channel.id();
        String consumerId = channelId.asLongText(); // 通道的长文本唯一 ID，避免重复
// 2. 构造 ACK 请求 DTO
        ConsumerAckReqDTO ackReq = new ConsumerAckReqDTO();
        ackReq.setMessageId(messageId);
        ackReq.setConsumerId(consumerId);
        ackReq.setAckStatus("SUCCESS"); // 处理成功，状态为 SUCCESS
// 3. 封装为 RPC 消息，发送给 Broker
        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true); // 这是消费者向 Broker 的请求
        rpcDto.setTraceId(String.valueOf(System.currentTimeMillis())); // 生成临时 traceId
        rpcDto.setMethodType(MethodType.C_ACK_MSG); // 协议类型：消费确认
        rpcDto.setJson(JSON.toJSONString(ackReq));
// 发送 ACK 消息
        channel.writeAndFlush(JSON.toJSONString(rpcDto) + "\n");
        log.info("GuoGuomq 消费者向 Broker 发送 ACK：消息 ID={}，消费者 ID={}", messageId, consumerId);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error ("GuoGuomq 消费者通道异常", cause);
        ctx.close (); // 异常时关闭通道
    }
}
