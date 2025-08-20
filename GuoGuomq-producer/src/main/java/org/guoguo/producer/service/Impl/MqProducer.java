package org.guoguo.producer.service.Impl;

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.MqMessage;
import org.guoguo.common.RpcMessageDTO;
import org.guoguo.common.constant.MethodType;
import org.guoguo.producer.constant.ResultCodeEnum;
import org.guoguo.producer.handler.MqProducerHandler;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.IMqProducer;
import org.guoguo.producer.util.SnowflakeIdGeneratorUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Slf4j
public class MqProducer implements IMqProducer {
    private String brokerAddress = "127.0.0.1:9999";
    private Channel channel;
    private String traceId;
    private String response;
    private final SnowflakeIdGeneratorUtil snowflakeIdGeneratorUtil = new SnowflakeIdGeneratorUtil();

    //用于启动消息生产者并连接到消息代理（broker）
    public void start() {
        String[] addr = brokerAddress.split(":");
        // broker地址
        String host = addr[0];
        // broker端口
        int port = Integer.parseInt(addr[1]);

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            log.info("GuoGuomq初始化中 生产者尝试连接broker====================>");
                            ch.pipeline()
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(new MqProducerHandler(MqProducer.this)); // 生产者处理器
                        }
                    });
            // 连接到指定的host和port，并等待连接完成
            ChannelFuture future = bootstrap.connect(host, port).sync();
            // 保存连接的Channel引用，供后续使用
            this.channel = future.channel();
        } catch (Exception e) {
            log.error("GuoGuomq初始化异常 生产者尝试连接broker失败====================>",e);
        }
    }
    //todo 生产者应该保持与broker的连接，而不是每次发送消息后都关闭连接。所以更好的做法可能是在MqProducer类中添加一个专门的关闭方法，在应用程序结束时调用，而不是在每次发送消息后关闭。
    @Override
    public Result<String> send(MqMessage message) {
        try {
            traceId= String.valueOf(snowflakeIdGeneratorUtil.nextId());
            RpcMessageDTO rpcMessageDTO=new RpcMessageDTO();
            rpcMessageDTO.setTraceId(traceId);
            rpcMessageDTO.setRequest(true);
            rpcMessageDTO.setMethodType(MethodType.P_SEND_MSG);
            rpcMessageDTO.setJson(JSON.toJSONString(message));

            //发送消息
            channel.writeAndFlush(JSON.toJSONString(rpcMessageDTO));
            //当有别的线程调用latch减一 为0后才会继续执行 不然就会等待超过超时时间 才继续执行
            CountDownLatch latch = new CountDownLatch(1);
            latch.await(4, TimeUnit.SECONDS);


            return Result.ok("消息发送成功",traceId);

        }catch (Exception e){
        return Result.build("消息发送失败", ResultCodeEnum.FAILED, traceId);
        }
    }
    // 用于接收响应
    public void setResponse(String traceId, String response) {
        if (this.traceId.equals(traceId)) {
            this.response = response;
        }
    }
}
