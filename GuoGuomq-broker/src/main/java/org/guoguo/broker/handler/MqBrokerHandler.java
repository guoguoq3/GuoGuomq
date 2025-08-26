package org.guoguo.broker.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;

@Slf4j
public class MqBrokerHandler extends SimpleChannelInboundHandler<String> {
    // 获取Broker管理器实例
    private final BrokerManager brokerManager = BrokerManager.getInstance();
   /**
     * 接收消息将rpc消息转为message对象
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        // 解析请求
        RpcMessageDTO rpcDto = JSON.parseObject(msg, RpcMessageDTO.class);
        log.info("Broker收到消息：" + rpcDto);
        // 根据方法类型处理不同请求
        switch (rpcDto.getMethodType()) {
            case MethodType.P_SEND_MSG:
                // 处理生产者发送的消息
                handleSendMessage(rpcDto, ctx);
                break;
            case MethodType.C_SUBSCRIBE:
                // 处理消费者的订阅请求
                handleSubscribe(rpcDto, ctx);
                break;
            default:
                log.error("Broker收到未知请求：" + rpcDto);
            // 这里后续会添加消息存储和推送逻辑
        }

        // 响应结果 todo：这里待完善 换为一个专门的响应体

    }

    // //处理消费者订阅消息 并告知消费者消息已收到 ChannelHandlerContext ctx与消费者通道交互

    private void handleSubscribe(RpcMessageDTO rpcDto, ChannelHandlerContext ctx) {
        // 解析订阅请求
        SubscribeReqDTO subscribeReqDTO = JSON.parseObject(rpcDto.getJson(), SubscribeReqDTO.class);
        log.info("GuoGuomq===============>Broker收到订阅请求：" + subscribeReqDTO);
        // 处理订阅请求
        brokerManager.handlerSubscribe(subscribeReqDTO, ctx.channel());

        //这里直接返回成功 不再包装一层了
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setTraceId(rpcDto.getTraceId());
        response.setJson("订阅成功"); // 保持字符串响应
        response.setMethodType(MethodType.B_SUBSCRIBE_RESPONSE); // 使用新类型
        ctx.writeAndFlush(JSON.toJSONString(response));
    }
    //处理生产者发送消息 并告知生产者消息已收到
    private void handleSendMessage(RpcMessageDTO rpcDto, ChannelHandlerContext ctx) {
        MqMessage message = JSON.parseObject(rpcDto.getJson(), MqMessage.class);
        log.info("GuoGuomq================>   Broker收到消息：" + message);
        // 处理消息
       brokerManager.handlerMessage(message);

        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setTraceId(rpcDto.getTraceId());
        response.setJson("SUCCESS");
        //todo 类型
        response.setMethodType(MethodType.B_PUSH_MSG);
        ctx.writeAndFlush(JSON.toJSONString(response));
    }
}