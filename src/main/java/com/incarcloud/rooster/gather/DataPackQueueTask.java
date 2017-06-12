package com.incarcloud.rooster.gather;/**
 * Created by Administrator on 2017/6/7.
 */

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.ERespReason;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.mq.MqSendResult;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.concurrent.BlockingQueue;

/**
 * @author Fan Beibei
 * @ClassName: DataPackQueueTask
 * @Description: 处理host数据包队列的任务
 * @date 2017-06-07 16:34
 */
public class DataPackQueueTask implements  Runnable{
    private static Logger s_logger = LoggerFactory.getLogger(DataPackQueueTask.class);



    /**
     * 任务名称（标识）
     */
    private String name;

    private GatherHost host;

    private BlockingQueue<DataPackWrap> dataPackQueue;

    /**
     *
     * @param dataPackQueue 要取数据的队列
     * @param host 所在主机
     */
    public DataPackQueueTask(BlockingQueue<DataPackWrap> dataPackQueue,GatherHost host){
        this.host = host;
        this.name = host.getName()+"-DataPackQueueTask"+ Calendar.getInstance().getTimeInMillis();
        this.dataPackQueue = dataPackQueue;
    }


    /**
     *
     * @param name 任务名称（标识）
     * @param dataPackQueue 要取数据的队列
     * @param host 所在主机
     */
    public DataPackQueueTask(String name,BlockingQueue<DataPackWrap> dataPackQueue,GatherHost host){
        this.host = host;
        this.name = name;
        this.dataPackQueue = dataPackQueue;
    }


    @Override
    public void run() {
        DataPackWrap packWrap =  dataPackQueue.poll();
        if (null == packWrap){//没取到数据包
            s_logger.debug("get no pack from queue!!");
            return;
        }

        DataPack pack = packWrap.getDataPack();

        try {
            //
            MQMsg mqMsg = new MQMsg();
            mqMsg.setMark(pack.getMark());
            mqMsg.setData(pack.getDataB64().getBytes());

            s_logger.debug("&&&&&&" + mqMsg);


            MqSendResult sendResult = host.getBigMQ().post(mqMsg);

            if (null == sendResult.getException()) {// 正常返回
                s_logger.info("success send to MQ:" + sendResult.getData());

                ByteBuf resp = packWrap.getDataParser().createResponse(pack, ERespReason.OK);
                 packWrap.getChannel().writeAndFlush(resp);

            } else {
                s_logger.error("failed send to MQ:" + sendResult.getException().getMessage());

                ByteBuf resp = packWrap.getDataParser().createResponse(pack, ERespReason.Failed);
                packWrap.getChannel().writeAndFlush(resp);
            }


        }catch (Exception e)
        {
            s_logger.error(e.getMessage());
        }finally {
            if(null != pack){
                pack.freeBuf();
            }
        }




    }
}
