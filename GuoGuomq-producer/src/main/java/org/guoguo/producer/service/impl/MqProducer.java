// org.guoguo.producer.service.impl.MqProducer.java
package org.guoguo.producer.service.impl;

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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.common.pojo.Entity.MqMessage;
import org.guoguo.common.config.MqConfigProperties;
import org.guoguo.common.constant.MethodType;
import org.guoguo.common.pojo.DTO.RpcMessageDTO;
import org.guoguo.producer.constant.ResultCodeEnum;
import org.guoguo.producer.handler.MqProducerHandler;
import org.guoguo.producer.pojo.Result;
import org.guoguo.producer.service.IMqProducer;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component // 交给 Spring 管理
public class MqProducer implements IMqProducer {
    private final MqConfigProperties config;
    private Channel channel;
    private EventLoopGroup group;
    private String traceId;

    private String response;
    private final SnowflakeIdGeneratorUtil snowflakeIdGeneratorUtil = new SnowflakeIdGeneratorUtil();

    // 注入配置
    @Autowired
    public MqProducer(MqConfigProperties config) {
        this.config = config;
    }

    // 初始化时连接 Broker
    @PostConstruct
    public void start() {
        group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            ch.pipeline()
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(new MqProducerHandler(MqProducer.this));
                        }
                    });

            // 从配置中获取 Broker 地址和端口
            ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
            this.channel = future.channel();
            log.info("生产者连接 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
        } catch (Exception e) {
            log.error("生产者连接 Broker 失败", e);
        }
    }

    @Override
    public Result<String> send(MqMessage message) {
        try {
            traceId = String.valueOf(snowflakeIdGeneratorUtil.nextId());
            RpcMessageDTO rpcMessageDTO = new RpcMessageDTO();
            rpcMessageDTO.setTraceId(traceId);
            rpcMessageDTO.setRequest(true);
            rpcMessageDTO.setMethodType(MethodType.P_SEND_MSG);
            rpcMessageDTO.setJson(JSON.toJSONString(message));

            channel.writeAndFlush(JSON.toJSONString(rpcMessageDTO));

            // 使用配置中的超时时间
            CountDownLatch latch = new CountDownLatch(1);
            latch.await(config.getProducerTimeout(), TimeUnit.MILLISECONDS);

            return Result.ok("消息发送成功", traceId);
        } catch (Exception e) {
            return Result.build("消息发送失败", ResultCodeEnum.FAILED, traceId);
        }
    }

    // 关闭资源
    @PreDestroy
    public void close() {
        if (group != null) group.shutdownGracefully();
        log.info("生产者已关闭");
    }

    public void setResponse(String traceId, String response) {
        if (this.traceId.equals(traceId)) {
            this.response = response;
        }
    }
}