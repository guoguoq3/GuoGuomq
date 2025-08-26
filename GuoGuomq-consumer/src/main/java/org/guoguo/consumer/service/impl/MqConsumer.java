// org.guoguo.consumer.service.impl.MqConsumer.java
package org.guoguo.consumer.service.impl;

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
import org.guoguo.common.pojo.DTO.SubscribeReqDTO;
import org.guoguo.consumer.handler.MqConsumerHandler;
import org.guoguo.consumer.service.IMessageListener;
import org.guoguo.consumer.service.IMqConsumer;
import org.guoguo.common.util.SnowflakeIdGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component // 交给 Spring 管理
public class MqConsumer implements IMqConsumer {
    private final MqConfigProperties config;
    private Channel channel;
    private EventLoopGroup group;
    private final Map<String, IMessageListener> topicListenerMap = new HashMap<>();
    private final SnowflakeIdGeneratorUtil snowflakeIdGeneratorUtil = new SnowflakeIdGeneratorUtil();

    // 注入配置
    @Autowired
    public MqConsumer(MqConfigProperties config) {
        this.config = config;
    }

    // 初始化时连接 Broker
    @PostConstruct
    @Override
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
                                    .addLast(new MqConsumerHandler(MqConsumer.this));
                        }
                    });

            // 从配置中获取 Broker 地址和端口
            ChannelFuture future = bootstrap.connect(config.getBrokerHost(), config.getBrokerPort()).sync();
            this.channel = future.channel();
            log.info("消费者连接 Broker 成功：{}:{}", config.getBrokerHost(), config.getBrokerPort());
        } catch (Exception e) {
            log.error("消费者连接 Broker 失败", e);
        }
    }

    @Override
    public void subscribe(SubscribeReqDTO subscribeReqDto, IMessageListener listener) {
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("未连接到 Broker，请先调用 start() 方法");
        }

        String topic = subscribeReqDto.getTopic();
        topicListenerMap.put(topic, listener);

        RpcMessageDTO rpcDto = new RpcMessageDTO();
        rpcDto.setRequest(true);
        rpcDto.setTraceId(String.valueOf(snowflakeIdGeneratorUtil.nextId()));
        rpcDto.setMethodType(MethodType.C_SUBSCRIBE);
        rpcDto.setJson(JSON.toJSONString(subscribeReqDto));
        channel.writeAndFlush(JSON.toJSONString(rpcDto));
        log.info("消费者订阅主题：{}", topic);
    }

    // 关闭资源
    @PreDestroy
    @Override
    public void close() {
        if (group != null) group.shutdownGracefully();
        log.info("消费者已关闭");
    }

    public void handlerMessage(String topic, MqMessage message) {
        IMessageListener listener = topicListenerMap.get(topic);
        if (listener != null) {
            listener.onMessage(message);
        } else {
            log.error("无主题 {} 的监听器", topic);
        }
    }
}