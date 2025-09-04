package org.guoguo.broker.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.core.BrokerManager;
import org.guoguo.common.pojo.DTO.ConsumerAckReqDTO;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.common.pojo.Entity.MqMessageEnduring;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
//每次有新的连接调用这个 都会新建一个实例 以前不报错 是因为每次我都新建new一个
@ChannelHandler.Sharable
public class MqBrokerHandler extends SimpleChannelInboundHandler<String> {
    // 获取Broker管理器实例
    private final BrokerManager brokerManager;

    @Autowired
    public MqBrokerHandler(BrokerManager brokerManager) {
        this.brokerManager = brokerManager;
    }
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
                MqMessageEnduring mqMessage=JSON.parseObject(rpcDto.getJson(), MqMessageEnduring.class);
                log.info("GuoGuomq================>   Broker收到消息：" + mqMessage);
                //处理生产者发送消息 并返回确认告知生产者消息已收到,
                brokerManager.handlerMessage(mqMessage,rpcDto.getTraceId());
                sendSuccessResponse(MethodType.P_CONFIRM_MSG, ctx,rpcDto.getTraceId());
                break;
            case MethodType.C_SUBSCRIBE:
                SubscribeReqDTO subscribeReqDTO = JSON.parseObject(rpcDto.getJson(), SubscribeReqDTO.class);
                log.info("GuoGuomq===============>Broker收到订阅请求：" + subscribeReqDTO);
                //处理消费者订阅消息 并告知消费者消息已收到 ChannelHandlerContext ctx与消费者通道交互
                brokerManager.handlerSubscribe(subscribeReqDTO, ctx.channel());
                sendSuccessResponse(MethodType.B_SUBSCRIBE_RESPONSE, ctx,rpcDto.getTraceId());
                brokerManager.backtrackHistoricalMessages(subscribeReqDTO.getTopic(),ctx.channel());
                break;
            case MethodType.C_ACK_MSG:
                ConsumerAckReqDTO ackReq = JSON.parseObject(rpcDto.getJson(), ConsumerAckReqDTO.class);
                brokerManager.handlerConsumerAck(ackReq, ctx.channel());
                break;
//                sendSuccessResponse(rpcDto.getMethodType(), ctx,rpcDto.getTraceId());
                // 处理消费者确认消息
            default:
                log.error("Broker收到未知请求：" + rpcDto);
            // 这里后续会添加消息存储和推送逻辑
        }

        // 响应结果 todo：这里待完善 换为一个专门的响应体

    }

    //

    private void  sendSuccessResponse(String methodType, ChannelHandlerContext ctx, String traceId) {

        //这里直接返回成功 不再包装一层了
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setTraceId(traceId);
        response.setJson("SUCCESS"); // 保持字符串响应
        response.setMethodType(methodType); // 使用新类型
        ctx.writeAndFlush(JSON.toJSONString(response) + "\n");
    }




}