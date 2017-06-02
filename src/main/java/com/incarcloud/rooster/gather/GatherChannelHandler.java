package com.incarcloud.rooster.gather;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.LoggerFactory;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 采集器的通道处理类
 * 
 * @author 熊广化
 *
 */
public class GatherChannelHandler extends ChannelInboundHandlerAdapter {
	private static org.slf4j.Logger s_logger = LoggerFactory.getLogger(GatherChannelHandler.class);
	/**
	 * 处理数据推送到mq的线程池
	 */
	private ExecutorService _threadPool;
    
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
    GatherChannelHandler(GatherSlot slot){
        _slot = slot;
        _parser = slot.getDataParser();
        
        _threadPool = Executors.newFixedThreadPool(20);
    }
    
    
    
    /**
     * @param slot 采集槽
     * @param threadPool 处理数据推送到mq的线程池
     */
    GatherChannelHandler(GatherSlot slot,ExecutorService threadPool){
        _slot = slot;
        _parser = slot.getDataParser();
        
        _threadPool = threadPool;
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
    	
    	Channel channel = ctx.channel();
//    	_threadPool.execute(new DataPachReadTask(channel, buf,_parser));
    	_threadPool.execute(new DataPachReadTask(get_slot().getName()+"-DataPachReadTask"+new Date().getTime(),channel, buf,_parser));
    	
    }



	/** 
	 * 获取 所属的采集槽 
	 * @return _slot 
	 */
	public GatherSlot get_slot() {
		return _slot;
	}
	
}