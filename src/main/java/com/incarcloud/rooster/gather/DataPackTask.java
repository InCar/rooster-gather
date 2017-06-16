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
public class DataPackTask {
    private static Logger s_logger = LoggerFactory.getLogger(DataPackTask.class);
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
    public DataPackTask(Channel channel, IDataParser dataParser, DataPack dataPack) {
        if (null == channel || null == dataParser || null == dataPack) {
            throw new IllegalArgumentException("DataPackTask constructor null param!");
        }

        this.channel = channel;
        this.dataParser = dataParser;
        this.dataPack = dataPack;
    }

    /**
     * 发送数据包到消息中间件
     *
     * @param iBigMQ 操作消息中间件接口
     */
    public void sendDataPackToMQ(IBigMQ iBigMQ) {
        if (null == iBigMQ) {
            throw new IllegalArgumentException("IBigMQ  is null !");
        }

        try {
            //
            MQMsg mqMsg = new MQMsg();
            mqMsg.setMark(dataPack.getMark());
            mqMsg.setData(dataPack.getDataB64().getBytes());

            s_logger.debug("&&&&&&" + mqMsg);


            MqSendResult sendResult = iBigMQ.post(mqMsg);

            if (null == sendResult.getException()) {// 正常返回
                s_logger.info("success send to MQ:" + sendResult.getData());

                ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.OK);

//                System.out.println("success***********" + dataPack + "   " + resp);
                if (null != resp) {//需要回应设备
                    channel.writeAndFlush(resp);
                }


            } else {
                s_logger.error("failed send to MQ:" + sendResult.getException().getMessage());
                ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.ERROR);

                if (null != resp) {//需要回应设备
                    channel.writeAndFlush(resp);
                }
            }

        } catch (Exception e) {
            s_logger.error("sendDataPackToMQ " + e.getMessage());
        } finally {
            if (null != dataPack) {
                dataPack.freeBuf();
            }
        }
    }

    public void destroy() {
        dataPack.freeBuf();
    }
}
