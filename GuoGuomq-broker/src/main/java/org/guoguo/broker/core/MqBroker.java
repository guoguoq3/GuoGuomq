package org.guoguo.broker.core;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.guoguo.broker.handler.MqBrokerHandler;

/**
 * broker信息连接通道
 */
@Slf4j
public class MqBroker extends Thread{
    private int port=9999;

    @Override
    public void run() {
        /*
        创建事件循环组的方法 主事件循环组 主要负责连接请求 将建立好的连接注册到wokergroup中的某个eventloop
        workerGroup 主要负责处理IO事件 每个连接channel会绑定一个eventloop 该
         */
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            //配置服务端网络通信
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    //服务器使用的通道类型
                    .channel(NioServerSocketChannel.class)
                    //当服务器接收到客户端连接后，会创建一个新的子通道（SocketChannel），childHandler 用于初始化该子通道的处理器链（ChannelPipeline）。
                    //ChannelInitializer 是一个特殊的处理器，仅在通道初始化时执行一次 initChannel 方法，之后会从处理器链中移除。
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            log.info("GuoGuomq初始化中，初始化broker消息处理器====================>");
                            channel.pipeline()
                                    //字符串解码加密
                                    .addLast(new StringDecoder())
                                    .addLast(new StringEncoder())
                                    .addLast(new MqBrokerHandler());

                        }
//                        SO_BACKLOG参数指定了可以排队等待接受的最大连接数
                    }).option(ChannelOption.SO_BACKLOG, 128)
                    //心跳包监测
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            //绑定端口并启动 sync()使当前端口阻塞
            ChannelFuture future = bootstrap.bind(port).sync();
            log.info("broker启动成功 监听端口:{}",port);
            //等待服务器监听端口关闭
            future.channel().closeFuture().sync();
            log.info("broker 关闭完成");
        }catch (Exception e){
            log.error("broker启动异常",e);
        }finally {
            //优雅的关闭
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }



    }

    public static void main(String[] args) {
        new MqBroker().start();
    }
}
