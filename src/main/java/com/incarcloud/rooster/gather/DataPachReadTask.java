package com.incarcloud.rooster.gather;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.ERespReason;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.mq.MqSendResult;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * 处理读取的数据包的任务
 *
 * @author 范贝贝
 */

@Deprecated
public class DataPachReadTask implements Runnable {
    private static Logger s_logger = LoggerFactory.getLogger(DataPachReadTask.class);

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
     * 发送消息接口
     */
    private IBigMQ bigMQ;

    /**
     * @param channel    nio 通道
     * @param binaryData 接受到的二进制数据
     * @param parser     数据转换器
     * @param bigMQ      发布消息的接口
     */
    public DataPachReadTask(Channel channel, ByteBuf binaryData, IDataParser parser, IBigMQ bigMQ) {
        this.name = "-DataPachReadTask" + new Date().getTime();
        this.channel = channel;
        this.binaryData = binaryData;
        this.parser = parser;
        this.bigMQ = bigMQ;
    }

    /**
     * @param name       任务名称（线程的标识）
     * @param channel    nio 通道
     * @param binaryData 接受到的二进制数据
     * @param parser     数据转换器
     * @param bigMQ      发布消息的接口
     */
    public DataPachReadTask(String name, Channel channel, ByteBuf binaryData, IDataParser parser, IBigMQ bigMQ) {
        this.name = name;
        this.channel = channel;
        this.binaryData = binaryData;
        this.parser = parser;
        this.bigMQ = bigMQ;
    }

    @Override
    public void run() {
        s_logger.debug(binaryData.toString());
        // 1、解析包
        List<DataPack> listPacks = null;
        try {
            listPacks = parser.extract(binaryData);
			binaryData.discardSomeReadBytes();

            s_logger.debug("-------------parsed--------------");


            if (null == listPacks || 0 == listPacks.size()) {
                s_logger.info("no packs!!");
                return;
            }

            // 2、发送到消息队列
            List<MQMsg> msgList = new ArrayList<>(listPacks.size());

            for (DataPack pack : listPacks) {
                MQMsg mqMsg = new MQMsg();
                mqMsg.setMark(pack.getMark());
                mqMsg.setData(pack.getDataB64().getBytes());

                msgList.add(mqMsg);

                s_logger.debug(name + "&&&&&&" + mqMsg);

            }

            List<MqSendResult> resultList = bigMQ.post(msgList);


            //3.回应设备
            for (int i = 0; i < resultList.size(); i++) {
                MqSendResult result = resultList.get(i);
                if (null == result.getException()) {// 正常返回
                    s_logger.debug("success send to MQ:" + result.getData());

                    ByteBuf resp = parser.createResponse(listPacks.get(i), ERespReason.OK);
                    channel.writeAndFlush(resp);

                } else {
                    s_logger.error("failed send to MQ:" + result.getException().getMessage());

                    ByteBuf resp = parser.createResponse(listPacks.get(i), ERespReason.Failed);
                    channel.writeAndFlush(resp);
                }
            }

        } catch (Exception e) {
            s_logger.debug(e.getMessage());

        } finally {
            // 释放内存
            if (null != listPacks) {
                for (DataPack pack : listPacks) {

                    pack.freeBuf();
                }
            }
        }

    }

    /**
     * 获取 任务名称（线程的标识）
     *
     * @return name
     */
    public String getName() {
        return name;
    }

}
