package com.incarcloud.rooster.gather.cmd.server;/**
 * Created by fanbeibei on 2017/7/17.
 */

import com.incarcloud.rooster.gather.GatherHost;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

/**
 * @author Fan Beibei
 * @Description: netty实现的http restful服务端
 * @date 2017/7/17 11:48
 */
public class NettyRestfulCommandServer extends  AbstractRestfulCommandServer{
    private static Logger s_logger = LoggerFactory.getLogger(NettyRestfulCommandServer.class);

    private GatherHost host;

    EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
    EventLoopGroup workerGroup = new NioEventLoopGroup();


    public NettyRestfulCommandServer(GatherHost host,int port){
        super(port);
        this.host = host;
    }


    @Override
    public String getUrl() throws UnknownHostException {
        return super.getUrl()+"/rest";
    }

    @Override
    public void start() {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel> (){
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast(new HttpServerCodec());/*HTTP 服务的解码器*/
                    p.addLast(new HttpObjectAggregator(2048));/*HTTP 消息的合并处理*/
                    p.addLast(new HttpRestfulHandler(new CommandService())); /*自己写的服务器逻辑处理*/
                }
            });

            Channel ch = b.bind(port).sync().channel();

            s_logger.info(host.getName()+"-command server start,listen on port:"+port);

            ch.closeFuture().sync();

        }catch (InterruptedException e) {
            e.printStackTrace();

        } finally {
            stop();
        }
    }

    @Override
    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        s_logger.info(host.getName()+"-command server stop!");
    }
}
