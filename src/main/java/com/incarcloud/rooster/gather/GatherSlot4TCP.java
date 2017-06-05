package com.incarcloud.rooster.gather;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * TCP协议的采集处理槽
 * 
 * @author 熊广化
 *
 */
class GatherSlot4TCP extends GatherSlot {
    private static final int BACKLOG_COUNT = 1024;
    private int _port;
    private Channel _channel;
    private ServerBootstrap _bootstrap;

    /**
     * 
     * @param port 端口
     * @param host 宿主
     */
    GatherSlot4TCP(int port, GatherHost host){
        super(host);
        GatherSlot _this = this;
        _port = port;

        _bootstrap = new ServerBootstrap();
        _bootstrap.group(host.getBossGroup(), host.getWorkerGroup());
        _bootstrap.channel(NioServerSocketChannel.class);
        _bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new GatherChannelHandler(_this));
            }
        });
        _bootstrap.option(ChannelOption.SO_BACKLOG, BACKLOG_COUNT);
        _bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    void start(){
        ChannelFuture future = _bootstrap.bind(_port);
        _channel = future.channel();
        _bootstrap = null; // 可以释放掉了
        try {
            future.sync();
        }
        catch (InterruptedException ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    void stop(){
        try {
            _channel.closeFuture().sync();
        }
        catch (InterruptedException ex){
            throw new RuntimeException(ex);
        }
    }
}
