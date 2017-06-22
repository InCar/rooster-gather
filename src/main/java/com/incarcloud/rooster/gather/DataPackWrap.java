package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.ERespReason;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.mq.MqSendResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

}
