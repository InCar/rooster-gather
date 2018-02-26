package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.cache.ICacheManager;
import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.ERespReason;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.gather.remotecmd.session.Session;
import com.incarcloud.rooster.gather.remotecmd.session.SessionFactory;
import com.incarcloud.rooster.mq.*;
import com.incarcloud.rooster.share.Constants;
import com.incarcloud.rooster.util.GsonFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Fan Beibei
 * @Description: 数据包发送器
 * @date 2017-06-07 17:34
 */
public class DataPackPostManager {

    /**
     * Logger
     */
    private static Logger s_logger = LoggerFactory.getLogger(DataPackPostManager.class);

    /**
     * 执行定时监控运行状态的线程池
     */
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    /**
     * 定时消费
     */
    private ScheduledExecutorService consumerExecutorService = Executors.newScheduledThreadPool(1);

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

    /**
     * 名称
     */
    private String name;
    /**
     * 所属主机
     */
    private GatherHost host;

    /**
     * 默认的 缓存队列大小
     */
    private static final int DEFULT_CACHE_QUEUE_SIZE = 10000;
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


    private static final int BATCH_GET_SIZE = 100 ;

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
            s_logger.error("Put to cacheQueue interrupted, {}", ExceptionUtils.getMessage(e));
        }
    }

    /**
     * 启动
     */
    public void start() {
        for (int i = 0; i < threadCount; i++) {
            new Thread(threadGroup, new QueueConsumerThread(host.getBigMQ(), host.getCacheManager())).start();
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


        /**
         * 定时消费MQ消息
         * 每秒定时处理，及时消费
         */
        consumerExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                IBigMQ bigMQ = host.getBigMQ() ;
                try {
                    // 循环获取MQ消息
                    while (true) {
                        List<byte[]> mqMsgs = bigMQ.batchReceive(host.getRemoteTopic(),BATCH_GET_SIZE);

                        //处理MQ消息逻辑----响应给TBOX
                        if (null != mqMsgs && mqMsgs.size() > 0){
                            for (byte[] mqMsg : mqMsgs){
                                try {
                                    String json = new String(mqMsg);
                                    s_logger.info("remote msg :{}",json);
                                    RemoteCmdSendMsg remoteCmdSendMsg = GsonFactory.newInstance().createGson().fromJson(json, RemoteCmdSendMsg.class);
                                    if (null == remoteCmdSendMsg)continue;

                                    String deviceId = remoteCmdSendMsg.getDeviceId() ;
                                    if (StringUtils.isBlank(deviceId))continue;

                                    byte[] bytes = Base64.getDecoder().decode(remoteCmdSendMsg.getCmdString()) ;
                                    Integer packId = remoteCmdSendMsg.getPackId() ;

                                    //根据绑定的设备ID获取Session
                                    String sessionId = SessionFactory.getInstance().getSessionId(deviceId) ;

                                    s_logger.info("sessionId :{}",sessionId);
                                    if (null != sessionId){
                                        // sessionId 与 Session 对应，如果sessionId 存在，Session不存在，说明有异常
                                        Session session = SessionFactory.getInstance().getSession(sessionId) ;
                                        s_logger.info("session :{}",session);
                                        if (null == session) {
                                            s_logger.error("Session is nonexistence ：deviceId:{},sessionId:{}",deviceId,sessionId);
                                            continue;
                                        }
                                        session.write(bytes).addListener(channelFuture->{
                                            if (channelFuture.isSuccess()){
                                                s_logger.info("send msg to T-Box Success :deviceId:{},bytes:{}",deviceId,new String(bytes));
                                                RemoteCmdFeedbackMsg feedbackMsg = new RemoteCmdFeedbackMsg(deviceId, packId, 1) ;
                                                bigMQ.post(host.getFeedBackTopic(),GsonFactory.newInstance().createGson().toJson(feedbackMsg).getBytes()) ;
                                            }else{
                                                s_logger.info("send msg to T-Box Error :deviceId:{},bytes:{}",deviceId,new String(bytes));
                                                RemoteCmdFeedbackMsg feedbackMsg = new RemoteCmdFeedbackMsg(deviceId, packId, 0) ;
                                                bigMQ.post(host.getFeedBackTopic(),GsonFactory.newInstance().createGson().toJson(feedbackMsg).getBytes()) ;
                                            }
                                        }) ;
                                    }
                                }catch (Exception e){
                                    s_logger.error("remote message err : {}",new String(mqMsg));
                                }
                            }
                        }
                        // 当查询出来的记录条数 小于 查询量，不再去循环查询
                        if (null == mqMsgs || mqMsgs.size() < BATCH_GET_SIZE){
                            break;
                        }
                    }
                }catch (Exception e){
                    System.out.println("No Consumer!!!!!!!!!");
                }
            }
        }, 1, 1, TimeUnit.SECONDS) ;

    }

    public void stop(){
        scheduledExecutorService.shutdownNow();

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

        private IBigMQ bigMQ;

        private ICacheManager cacheManager;

        public QueueConsumerThread(IBigMQ bigMQ, ICacheManager cacheManager) {
            if (null == bigMQ || null == cacheManager) {
                throw new IllegalArgumentException();
            }

            this.bigMQ = bigMQ;
            this.cacheManager = cacheManager;
        }

        @Override
        public void run() {

            s_logger.debug("## QueueConsumerThread  start !!!");

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
                            s_logger.debug("## One batch complete size: {}", packWrapList.size());
                            batchSendDataPackToMQ(packWrapList);
                            packWrapList.clear();
                        }

                        continue;
                    }

                    //没取到说明队列暂时没有数据，将取到的批量发送
                    if (packWrapList.size() > 0) {
                        s_logger.debug("## One batch complete size: {}", packWrapList.size());
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
            s_logger.debug("Batch send datapack size to MQ: {}", packWrapBatch.size());

            if (null == packWrapBatch || 0 == packWrapBatch.size()) {
                throw new IllegalArgumentException();
            }

            // 构建MQMsg消息体
            List<byte[]> msgList = new ArrayList<>(packWrapBatch.size());
            for (DataPackWrap packWrap : packWrapBatch) {
                DataPack dp = packWrap.getDataPack();
                try {
                	// 根据deviceId回复设备数据
                	String gatherMark = dp.getMark();
                	if(null != packWrap && null != packWrap.getMetaData()){
                        String deviceId = (String) packWrap.getMetaData().get(Constants.MetaDataMapKey.DEVICE_ID);
                		gatherMark += "|" + deviceId;
                	}

                	// 构建MQ消息体
                    MQMsg mqMsg = new MQMsg(gatherMark, dp.serializeToBytes());

                    msgList.add(GsonFactory.newInstance().createGson().toJson(mqMsg).getBytes());
                } catch (UnsupportedEncodingException e) {
                    s_logger.error("Plant not support UTF-8, {}", packWrap.getDataPack());
                }
            }

            // 发送消息
            List<MqSendResult> resultList = bigMQ.post(host.getDataPackTopic(), msgList);
            s_logger.debug("Result list size: {}", resultList.size());

            // 回应设备
            for (int i = 0; i < resultList.size(); i++) {
                MqSendResult sendResult = resultList.get(i);
                IDataParser dataParser = packWrapBatch.get(i).getDataParser();
                DataPack dataPack = packWrapBatch.get(i).getDataPack();
                Channel channel = packWrapBatch.get(i).getChannel();
                Map<String, Object> metaData = packWrapBatch.get(i).getMetaData();

                try {

                    if (null == sendResult.getException()) {// 正常返回
                        // 保存数据到MQ成功
                        s_logger.debug("Success send to MQ: {}", sendResult.getData());

                        // 激活流程
                        ByteBuf resp = null;
                        if(null != metaData) {
                            // 获得设备号
                            String deviceId = (String) metaData.get(Constants.MetaDataMapKey.DEVICE_ID);
                            int packType = (int) metaData.get(Constants.MetaDataMapKey.PACK_TYPE);

                            // 判断报文类型
                            switch (packType) {
                                case Constants.PackType.ACTIVATE:
                                    /* 激活数据包 */
                                    //　获得车辆标识
                                    String vin = (String) metaData.get(Constants.MetaDataMapKey.VIN);
                                    String cacheVin = cacheManager.get(Constants.CacheNamespace.CACHE_NS_DEVICE_CODE + deviceId);

                                    // 打印车辆信息
                                    s_logger.info("Activate: deviceId = {}, vin = {}, cacheVin = {}", deviceId, vin, cacheVin);

                                    // 判断deviceId和vin在缓存中是否匹配
                                    if(!StringUtils.equals(cacheVin, vin)) {
                                        // 激活失败，原因：TBox未安装到指定车辆
                                        resp = dataParser.createResponse(dataPack, ERespReason.MISMATCH);
                                    } else {
                                        // 激活成功，维护VIN找设备号
                                        if(StringUtils.isNotBlank(cacheVin)) {
                                            cacheManager.set(Constants.CacheNamespace.CACHE_NS_VEHICLE_VIN + cacheVin, deviceId);
                                        }
                                    }

                                    break;
                                case Constants.PackType.LOGIN:
                                    /* 登陆数据包 */
                                    break;
                                default:
                                    /* 非激活或登陆数据包 */
                            }
                        }

                        // 其他情况
                        if(null == resp) {
                            resp = dataParser.createResponse(dataPack, ERespReason.OK);
                        }
                        s_logger.debug("Success send resp: {}", ByteBufUtil.hexDump(resp));

                        // 需要回应设备
                        if (null != resp) {
                            channel.writeAndFlush(resp);
                        }
                    } else {
                        // 创建错误应答包
                        s_logger.error("Failed send to MQ:" + sendResult.getException().getMessage());
                        ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.ERROR);
                        s_logger.debug("Failed send resp: {}", ByteBufUtil.hexDump(resp));

                        // 需要回应设备
                        if (null != resp) {
                            channel.writeAndFlush(resp);
                        }
                    }

                } catch (Exception e) {
                    s_logger.error("Send datapack to MQ exception: {}", ExceptionUtils.getMessage(e));
                }
            }

            // 释放datapack
            for (DataPackWrap packWrap : packWrapBatch) {
                packWrap.getDataPack().freeBuf();
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
                DataPack dp = packWrap.getDataPack();
                MQMsg mqMsg = new MQMsg(dp.getMark(), dp.serializeToBytes());
                s_logger.debug("-->" + mqMsg);
                // TODO
                //s_logger.error(dataParser.getMetaData(dataPack.get));

                MqSendResult sendResult = bigMQ.post(host.getDataPackTopic(),GsonFactory.newInstance().createGson().toJson(mqMsg).getBytes()) ;

                if (null == sendResult.getException()) {// 正常返回
                    s_logger.debug("success send to MQ:" + sendResult.getData());
                    ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.OK);
                    s_logger.debug("success send resp:"+ByteBufUtil.hexDump(resp));

                    if (null != resp) {//需要回应设备
                        channel.writeAndFlush(resp);
                    }else{

                    }
                } else {
                    s_logger.error("failed send to MQ:" + sendResult.getException().getMessage());
                    ByteBuf resp = dataParser.createResponse(dataPack, ERespReason.ERROR);
                    s_logger.error("failed send resp:"+ByteBufUtil.hexDump(resp));

                    if (null != resp) {//需要回应设备
                        channel.writeAndFlush(resp);
                    }else{

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
