// org.guoguo.broker.core.MqBroker.java
package org.guoguo.broker.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.handler.MqBrokerHandler;
import org.guoguo.common.config.MqConfigProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component // 交给 Spring 管理
public class MqBroker {
    private final MqConfigProperties config;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    // 构造器注入配置
    @Autowired
    public MqBroker(MqConfigProperties config) {
        this.config = config;
    }

    // 启动时自动执行（初始化 Netty 服务）
    @PostConstruct
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel channel) {
                            channel.pipeline()
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(new MqBrokerHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 从配置中获取端口
            ChannelFuture future = bootstrap.bind(config.getBrokerPort()).sync();
            log.info("Broker 启动成功，监听端口: {}", config.getBrokerPort());
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    // 销毁时关闭资源
    @PreDestroy
    public void stop() {
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
        log.info("Broker 已关闭");
    }
}