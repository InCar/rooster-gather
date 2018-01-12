package com.incarcloud.rooster.gather.remotecmd.session;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Kong on 2018/1/8.
 */
public class Session {

    private Logger logger = LoggerFactory.getLogger(Session.class) ;

    private String sessionId;
    private ChannelHandlerContext channelHandlerContext;

    public Session(String sessionId, ChannelHandlerContext ctx) {
        this.sessionId = sessionId;
        this.channelHandlerContext = ctx;
    }

    public ChannelFuture write(final byte[] buf) throws Exception {
        if (channelHandlerContext.channel().isOpen() && channelHandlerContext.channel().isActive() && channelHandlerContext.channel().isWritable()) {
            ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
            byteBuf.writeBytes(buf);
            return channelHandlerContext.channel().writeAndFlush(byteBuf).addListener((ChannelFutureListener) channelFuture -> {
                if (channelFuture.isSuccess()) {
                    logger.info("send msg Success : {}" , new String(buf));
                }else{
                    logger.error("send msg Fail : {}" , new String(buf));
                }
            });
        } else {
            logger.error("send msg error!!!");
            throw new Exception(String.format("send msg error!!!", this.getSessionId(), buf));
        }
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }

    public void setChannelHandlerContext(ChannelHandlerContext channelHandlerContext) {
        this.channelHandlerContext = channelHandlerContext;
    }

    @Override
    public String toString() {
        return "Session{" +
                "sessionId='" + sessionId + '\'' +
                ", channelHandlerContext=" + channelHandlerContext +
                '}';
    }
}
