package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.ERespReason;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.mq.MqSendResult;
import org.slf4j.LoggerFactory;

import com.incarcloud.rooster.datapack.IDataParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

/**
 * 采集器的通道处理类
 *
 * @author 熊广化
 */
public class GatherChannelHandler extends ChannelInboundHandlerAdapter {
    private static org.slf4j.Logger s_logger = LoggerFactory.getLogger(GatherChannelHandler.class);


    /**
     * 所属的采集槽
     */
    private GatherSlot _slot;

    /**
     * 累积缓冲区
     */
    private ByteBuf _buffer = null;
    private IDataParser _parser = null;

    /**
     * @param slot 采集槽
     */
    GatherChannelHandler(GatherSlot slot) {
        _slot = slot;
        _parser = slot.getDataParser();

    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        s_logger.error("{}", cause.toString());
        ctx.close();
    }

    private void OnRead(ChannelHandlerContext ctx, ByteBuf buf) {
        s_logger.debug("!!!!----" + _parser.getClass());

        Channel channel = ctx.channel();


        List<DataPack> listPacks = null;
        try {
            // 1、解析包
            listPacks = _parser.extract(buf);

            s_logger.debug("-------------parsed--------------");


            if (null == listPacks || 0 == listPacks.size()) {
                s_logger.info("no packs!!");
                return;
            }


            // 2、扔到host的消息队列
            for (DataPack pack:listPacks) {
                _slot.get_host().putToMsgQueue(new DataPackWrap(pack,channel,_parser));
                s_logger.debug("#####putToMsgQueue OK");
            }

        } catch (Exception e) {
            s_logger.debug(e.getMessage());

        } /*finally {
            // 释放内存
            if (null != listPacks || listPacks.size() > 0) {
                for (DataPack pack : listPacks) {

                    pack.freeBuf();
                }
            }
        }*/




//        Channel channel = ctx.channel();
//        get_slot().get_host().getMqThreadPool().execute(new DataPachReadTask(get_slot().getName() + "-DataPachReadTask" + Calendar.getInstance().getTimeInMillis(),
//                channel, buf, _parser, _slot.get_host().getBigMQ()));

    }

    /**
     * 获取 所属的采集槽
     *
     * @return _slot
     */
    public GatherSlot get_slot() {
        return _slot;
    }

}