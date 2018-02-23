package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import io.netty.channel.Channel;

import java.util.Map;

/**
 * Created by fanbeibei on 2017Da/6/15.
 */
public class DataPackWrap {

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
     * 设备报文Meta数据
     */
    private Map<String, Object> metaData;

    /**
     * 构造函数
     *
     * @param channel    会话通道
     * @param dataParser 数据转换器
     * @param dataPack   数据包
     * @param metaData   设备报文Meta数据
     */
    public DataPackWrap(Channel channel, IDataParser dataParser, DataPack dataPack, Map<String, Object> metaData) {
        if (null == channel || null == dataParser || null == dataPack) {
            throw new IllegalArgumentException();
        }

        this.channel = channel;
        this.dataParser = dataParser;
        this.dataPack = dataPack;
        this.metaData = metaData;
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

    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }
}
