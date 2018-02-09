package com.incarcloud.rooster.gather;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDP协议的采集处理槽
 * 
 * @author 熊广化
 *
 */
class GatherSlot4UDP extends GatherSlot{

    /**
     * Logger
     */
    private static Logger s_logger = LoggerFactory.getLogger(GatherSlot4UDP.class);

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
    protected void start0(){
        ChannelFuture future = _bootstrap.bind(_port);
        _channel = future.channel();

        _bootstrap = null; // 可以释放掉了
        try {
            future.sync();
            s_logger.info(getName()+" start success listen on port "+_port);
        }
        catch (InterruptedException ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void stop(){
        try {
            _channel.closeFuture().sync();
        }
        catch (InterruptedException ex){
            throw new RuntimeException(ex);
        }
    }

    @Override
    public String getTransportProtocal() {
        return "udp";
    }

    @Override
    public int getListenPort(){
        return _port;
    }
}
