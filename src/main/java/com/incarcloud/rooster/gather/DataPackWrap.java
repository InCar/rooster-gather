package com.incarcloud.rooster.gather;/**
 * Created by Administrator on 2017/6/7.
 */

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import io.netty.channel.Channel;


/**
 * @author Fan Beibei
 * @ClassName: DataPackWrap
 * @Description: 包裹DataPack及相关信息方便发送给消息队列的类
 * @date 2017-06-07 17:34
 */
public class DataPackWrap {
    /**
     * 数据包
     */
    private DataPack dataPack;
    /**
     * 接收数据包的netty通道
     */
    private Channel channel;
    /**
     * 解析数据包的解析器
     */
    private IDataParser dataParser;

    public DataPackWrap() {
    }

    /**
     *
     * @param dataPack 数据包
     * @param channel 接收数据包的netty通道
     * @param dataParser 解析数据包的解析器
     */
    public DataPackWrap(DataPack dataPack, Channel channel, IDataParser dataParser) {
        this.dataPack = dataPack;
        this.channel = channel;
        this.dataParser = dataParser;
    }

    public DataPack getDataPack() {
        return dataPack;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setDataPack(DataPack dataPack) {
        this.dataPack = dataPack;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public void setDataParser(IDataParser dataParser) {
        this.dataParser = dataParser;
    }

    public IDataParser getDataParser() {

        return dataParser;
    }


    @Override
    public String toString() {
        return "DataPackWrap{" +
                "dataPack=" + dataPack +
                ", channel=" + channel +
                ", dataParser=" + dataParser +
                '}';
    }
}
