package org.guoguo.broker.handler;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.MqMessage;
import org.guoguo.common.RpcMessageDTO;
import org.guoguo.common.constant.MethodType;
@Slf4j
public class MqBrokerHandler extends SimpleChannelInboundHandler<String> {
   /**
     * 接收消息将rpc消息转为message对象
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        // 解析请求
        RpcMessageDTO rpcDto = JSON.parseObject(msg, RpcMessageDTO.class);
        log.info("Broker收到消息：" + rpcDto);
        // 处理不同类型的请求
        if (MethodType.P_SEND_MSG.equals(rpcDto.getMethodType())) {
            // 处理生产者发送的消息
            MqMessage mqMessage = JSON.parseObject(rpcDto.getJson(), MqMessage.class);
            log.info("收到消息：" + mqMessage.getPayload());

            // 这里后续会添加消息存储和推送逻辑
        }

        // 响应结果 todo：这里待完善 换为一个专门的响应体
        RpcMessageDTO response = new RpcMessageDTO();
        response.setRequest(false);
        response.setTraceId(rpcDto.getTraceId());
        response.setJson("SUCCESS");
        ctx.writeAndFlush(JSON.toJSONString(response));
    }
}