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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Fan Beibei
 * @Description: 数据包发送器
 * @date 2017-06-07 17:34
 */
public class DataPackPostManager {
    private static Logger s_logger = LoggerFactory.getLogger(DataPackPostManager.class);

    /**
     * 执行定时监控运行状态的线程池
     */
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    /**
     * 截至现在入队数量
     */
    private AtomicInteger inQueueCount = new AtomicInteger(0);

    /**
     * 截至现在出队数量
     */
    private AtomicInteger outQueueCount = new AtomicInteger(0);

    /**
     * 上次统计的值-入队数量
     */
    private volatile int lastInQueueCount = 0;
    /**
     * 上次统计的值-出队数量
     */
    private volatile int lastOutQueueCount = 0;
    /**
     * 队列中堆积量
     */
    private volatile int remainQueueCount = 0;
    /**
     * 统计间隔时间
     */
    private int period = 20;


    private String name;
    /**
     * 所属主机
     */
    private GatherHost host;

    /**
     * 默认的 缓存队列大小
     */
    private static final int DEFULT_CACHE_QUEUE_SIZE = 1000;
    /**
     * 默认线程数
     */
    private static final int DEFAULT_THREAD_COUNT = 20;

    /**
     * 缓存队列,缓存数据包发送任务
     */
    private BlockingQueue<DataPackWrap> cacheQueue;
    /**
     * 消费线程数
     */
    private int threadCount;
    /**
     * 管理消费线程的线程组
     */
    private ThreadGroup threadGroup;


    /**
     * @param host 所属主机
     */
    public DataPackPostManager(GatherHost host) {
        this(host, DEFULT_CACHE_QUEUE_SIZE, DEFAULT_THREAD_COUNT);
    }

    /**
     * @param host           所属主机
     * @param cacheQueueSize 缓存队列大小
     * @param threadCount    执行任务线程数
     */
    public DataPackPostManager(GatherHost host, int cacheQueueSize, int threadCount) {
        if (null == host || cacheQueueSize <= 0 || threadCount <= 0)
            throw new IllegalArgumentException();

        this.host = host;
        this.threadGroup = new ThreadGroup(host.getName() + "-DataPackPostManager-ThreadGroup");

        this.name = host.getName() + "-DataPackPostManager";

        this.cacheQueue = new ArrayBlockingQueue<DataPackWrap>(cacheQueueSize);
        this.threadCount = threadCount;

    }


    /**
     * 添加任务 （线程安全）
     *
     * @param task
     */
    public void add(DataPackWrap task) {
        if (null == task) {
            return;
        }

        try {
            cacheQueue.put(task);
            inQueueCount.incrementAndGet();
        } catch (InterruptedException e) {
            //这里一般不会有中断触发这里
            task.destroy();
            s_logger.error("put to cacheQueue  interrapted", task, e.getMessage());
        }
    }


    /**
     * 启动
     */
    public void start() {
        for (int i = 0; i < threadCount; i++) {
            new Thread(threadGroup, new QueueConsumerThread(host.getBigMQ())).start();
        }


        //间隔一定时间统计队列状况
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                int _inQueueCount = inQueueCount.get();
                int _outQueueCount = outQueueCount.get();
                remainQueueCount = _inQueueCount - _outQueueCount;


                int newInQueueCount = _inQueueCount - lastInQueueCount;
                int newOutQueueCount = _outQueueCount - lastOutQueueCount;
                lastInQueueCount = _inQueueCount;
                lastOutQueueCount = _outQueueCount;

                s_logger.info(DataPackPostManager.this.name + " queue  contains  "
                        + remainQueueCount + " datapack for  send in queue,last " + period + " second " + newInQueueCount
                        + " in , " + newOutQueueCount + "  out  !");


            }
        }, period, period, TimeUnit.SECONDS);
    }


    /**
     * @author Fan Beibei
     * @Description: 取 发送任务 并执行的线程
     * @date 2017-06-07 17:34
     */
    private class QueueConsumerThread implements Runnable {
        /**
         * 批量发送到MQ的数量
         */
        private static final int BATCH_POST_SIZE = 16;
        private IBigMQ iBigMQ;


        public QueueConsumerThread(IBigMQ bigMQ) {
            if (null == bigMQ) {
                throw new IllegalArgumentException();
            }

            this.iBigMQ = bigMQ;
        }


        @Override
        public void run() {

            List<DataPackWrap> packWrapList = new ArrayList<DataPackWrap>(BATCH_POST_SIZE);

            while (true) {
                //取数据包
                try {

                    DataPackWrap packWrap = cacheQueue.poll();
                    if (null != packWrap) {
                        packWrapList.add(packWrap);
                        outQueueCount.incrementAndGet();

                        //取满一个批次了，直接发送完后再取下一个批次
                        if (BATCH_POST_SIZE == packWrapList.size()) {
                            batchSendDataPackToMQ(packWrapList);
                            packWrapList.clear();
                        }

                        continue;
                    }

                    //没取到说明队列暂时没有数据，将取到的批量发送
                    if (packWrapList.size() > 0) {
                        batchSendDataPackToMQ(packWrapList);
                        packWrapList.clear();
                    }

                    packWrap = cacheQueue.take();//阻塞住等待新数据，开始下一个批量
                    packWrapList.add(packWrap);
                    outQueueCount.incrementAndGet();

                } catch (InterruptedException e) {
                    s_logger.error(e.getMessage());
                }

            }


        }


        /**
         * 批量发送数据包到消息中间件
         *
         * @param packWrapBatch
         */
        protected void batchSendDataPackToMQ(List<DataPackWrap> packWrapBatch) {
            if (null == packWrapBatch || 0 == packWrapBatch.size()) {
                throw new IllegalArgumentException();
            }

            List<MQMsg> msgList = new ArrayList<>(packWrapBatch.size());
            for (DataPackWrap packWrap : packWrapBatch) {
                MQMsg mqMsg = new MQMsg();
                mqMsg.setMark(packWrap.getDataPack().getMark());
                mqMsg.setData(packWrap.getDataPack().getDataB64().getBytes());

                msgList.add(mqMsg);
            }


            List<MqSendResult> resultList = iBigMQ.post(msgList);


            for (int i = 0; i < resultList.size(); i++) {

                MqSendResult sendResult = resultList.get(i);
                IDataParser dataParser = packWrapBatch.get(i).getDataParser();
                DataPack dataPack = packWrapBatch.get(i).getDataPack();
                Channel channel = packWrapBatch.get(i).getChannel();

                try {

                    if (null == sendResult.getException()) {// 正常返回
                        s_logger.debug("success send to MQ:" + sendResult.getData());
                        ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.OK);

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
                }
            }

        }


        /**
         * 发送单个数据包到消息中间件
         */
        protected void sendDataPackToMQ(DataPackWrap packWrap) {
            if (null == packWrap) {
                throw new IllegalArgumentException();
            }


            DataPack dataPack = packWrap.getDataPack();
            IDataParser dataParser = packWrap.getDataParser();
            Channel channel = packWrap.getChannel();


            try {
                //
                MQMsg mqMsg = new MQMsg();
                mqMsg.setMark(dataPack.getMark());
                mqMsg.setData(dataPack.getDataB64().getBytes());
                s_logger.debug("&&&&&&" + mqMsg);


                MqSendResult sendResult = iBigMQ.post(mqMsg);

                if (null == sendResult.getException()) {// 正常返回
                    s_logger.debug("success send to MQ:" + sendResult.getData());
                    ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.OK);

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

    }

}
