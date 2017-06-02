package com.incarcloud.rooster.gather;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.mq.MqSendResult;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * 处理读取的数据包的任务
 * 
 * @author 范贝贝
 *
 */
public class DataPachReadTask implements Runnable{
	private static Logger s_logger = LoggerFactory.getLogger(GatherChannelHandler.class);
	
	/**
	 * 任务名称（线程的标识）
	 */
	private String name;
	
	/**
	 * nio 通道
	 */
	private Channel channel;
	/**
	 * 接受到的二进制数据
	 */
	private ByteBuf binaryData;
	/**
	 * 数据转换器
	 */
	private IDataParser parser;
	
	
	/**
	 * @param channel nio 通道
	 * @param binaryData 接受到的二进制数据
	 * @param parser 数据转换器
	 */
	public DataPachReadTask(Channel channel, ByteBuf binaryData, IDataParser parser) {
		this.name = "-DataPachReadTask"+new Date().getTime();
		this.channel = channel;
		this.binaryData = binaryData;
		this.parser = parser;
	}

	/**
	 * @param name 任务名称（线程的标识）
	 * @param channel nio 通道
	 * @param binaryData 接受到的二进制数据
	 * @param parser 数据转换器
	 */
	public DataPachReadTask(String name, Channel channel,ByteBuf binaryData, IDataParser parser) {
		this.name = name;
		this.channel = channel;
		this.binaryData = binaryData;
		this.parser = parser;
	}

	@Override
	public void run() {
		//1、解析包
		List<DataPack> listPacks = parser.extract(binaryData);
		binaryData.discardSomeReadBytes();

		if(null == listPacks){
			s_logger.info("no packs!!");
			return;
		}
		
		
		//2、发送到消息队列
        
		
		
		
		
		//释放内存
        for(DataPack pack : listPacks){
        	
            pack.freeBuf();
        }

        
		
	}
	
	
	/**
	 * 发送数据到
	 * @param listPacks
	 * @return
	 */
	private List<MqSendResult> postMQ(List<DataPack> listPacks){
		return null;
		
	}
	
	
	

	/** 
	 * 获取 任务名称（线程的标识） 
	 * @return name 
	 */
	public String getName() {
		return name;
	}
	

}
