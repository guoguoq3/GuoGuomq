package org.guoguo.consumer.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
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
            // ...
        } else if (MethodType.B_SUBSCRIBE_RESPONSE.equals(rpcDto.getMethodType())) {
            // 处理订阅响应
            log.info("订阅结果：{}", rpcDto.getJson());
        } else {
            log.warn("消费者收到未知类型的消息：{}", rpcDto.getMethodType());
        }
    }

}
