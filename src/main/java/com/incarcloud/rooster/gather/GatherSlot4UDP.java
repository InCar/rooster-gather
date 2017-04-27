package com.incarcloud.rooster.gather;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

class GatherSlot4UDP extends GatherSlot{
    private static final int BACKLOG_COUNT = 1024;
    private int _port;
    private Channel _channel;
    private Bootstrap _bootstrap;

    GatherSlot4UDP(int port, GatherHost host){
        super(host);
        GatherSlot _this = this;
        _port = port;

        _bootstrap = new Bootstrap();
        _bootstrap.group(host.getWorkerGroup());
        _bootstrap.channel(NioServerSocketChannel.class);
        _bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new GatherChannelHandler(_this));
            }
        });
        _bootstrap.option(ChannelOption.SO_BACKLOG, BACKLOG_COUNT);
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
