package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public enum GatherPortType{
    TCP, UDP, MQTT;
}

class GatherChannelHandler extends ChannelInboundHandlerAdapter {
    private static Logger s_logger = LoggerFactory.getLogger(GatherChannelHandler.class);
    private GatherSlot _slot;
    // 累积缓冲区
    private ByteBuf _buffer = null;
    private IDataParser _parser = null;

    GatherChannelHandler(GatherSlot slot){
        _slot = slot;
        _parser = slot.getDataParser();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg){
        ByteBuf buf = (ByteBuf)msg;
        _buffer.writeBytes(buf);
        buf.release();

        this.OnRead(ctx, _buffer);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        _buffer = ctx.alloc().buffer();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        _buffer.release();
        _buffer = null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
        s_logger.error("{}", cause.toString());
        ctx.close();
    }

    private void OnRead(ChannelHandlerContext ctx, ByteBuf buf){
        List<DataPack> listPacks = _parser.extract(buf);

        _slot.postMQ(listPacks).fail(listResults -> {
            // TODO: 从_parser找到一个错误回应包,回答给设备端
        });

        for(DataPack pack : listPacks){
            pack.freeBuf();
        }

        buf.discardSomeReadBytes();
    }
}


