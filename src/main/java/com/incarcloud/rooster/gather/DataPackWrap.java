package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import io.netty.channel.Channel;

/**
 * Created by fanbeibei on 2017Da/6/15.
 */
public class DataPackWrap {
	/**
	 * vin码
	 */
	private String vin;
    /**
     * 会话通道
     */
    private Channel channel;
    /**
     * 数据转换器
     */
    private IDataParser dataParser;
    /**
     * 数据包
     */
    private DataPack dataPack;


    /**
     * @param channel    会话通道
     * @param dataParser 数据转换器
     * @param dataPack   数据包
     */
    public DataPackWrap(Channel channel, IDataParser dataParser, DataPack dataPack) {
        if (null == channel || null == dataParser || null == dataPack) {
            throw new IllegalArgumentException();
        }

        this.channel = channel;
        this.dataParser = dataParser;
        this.dataPack = dataPack;
    }
    



    public void destroy() {
        dataPack.freeBuf();
    }

    public Channel getChannel() {
        return channel;
    }

    public IDataParser getDataParser() {
        return dataParser;
    }

    public DataPack getDataPack() {
        return dataPack;
    }


	/**  
	 * 获取vin  
	 * @return vin vin  
	 */
	public String getVin() {
		return vin;
	}




	/**  
	 * 设置vin  
	 * @param vin vin  
	 */
	public void setVin(String vin) {
		this.vin = vin;
	}
	
	

}
