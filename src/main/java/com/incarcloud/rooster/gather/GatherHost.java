package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.mq.IBigMQ;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 采集槽所在主机
 *
 * @author 熊广化
 */
public class GatherHost {
    private static Logger s_logger = LoggerFactory.getLogger(GatherHost.class);


    /**
     * 默认的 处理发送消息任务线程池大小
     */
    private static final int DEFULT_MQ_THREADPOOL_SIZE=20;
    /**
     * 默认的 缓存接收数据包的队列大小
     */
    private static final int DEFULT_DATA_PACK_QUEUE_SIZE=1000;

    /**
     * 主机名
     */
    private String name;



    private EventLoopGroup _bossGroup;
    private EventLoopGroup _workerGroup;

    /**
     * 处理数据推送到mq的线程池
     */
    private ExecutorService mqThreadPool;

    /**
     * 缓存接收数据包的队列
     */
    private ArrayBlockingQueue<DataPackWrap> dataPackQueue;


    /**
     * 采集槽列表
     */
    private ArrayList<GatherSlot> _slots = new ArrayList<>();

    /**
     * 操作消息队列接口
     */
    private IBigMQ bigMQ;


    /**
     * 是否已启动
     */
    private Boolean _bRunning = false;
    public GatherHost() {
        this("host"+Calendar.getInstance().getTimeInMillis(),DEFULT_MQ_THREADPOOL_SIZE,DEFULT_DATA_PACK_QUEUE_SIZE);
    }

    /**
     * @param name 主机名
     */
    public GatherHost(String name) {
        this(name,DEFULT_MQ_THREADPOOL_SIZE,DEFULT_DATA_PACK_QUEUE_SIZE);
    }

    /**
     * @param name 主机名
     * @param mqThreadPoolSize 处理发送消息任务线程池大小
     * @param msgQueueSize  缓存接收数据包的队列大小
     *
     */
    public GatherHost(String name,int mqThreadPoolSize,int msgQueueSize) {
        this.name = name;
        _bossGroup = new NioEventLoopGroup();
        _workerGroup = new NioEventLoopGroup();
        this.mqThreadPool = Executors.newFixedThreadPool(mqThreadPoolSize);
        this.dataPackQueue = new ArrayBlockingQueue<DataPackWrap>(msgQueueSize);

    }


    /**
     * 启动
     *
     * @throws Exception
     */
    public void start() throws Exception {
        if (_bRunning)
            return;

        //启动所有采集槽
        for (GatherSlot slot : _slots) {
            slot.start();
        }

        //启动处理数据包队列的线程

        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                s_logger.debug("check queue");
                while (true){
                    if(!dataPackQueue.isEmpty()){
                        DataPackQueueTask task = new DataPackQueueTask(dataPackQueue,GatherHost.this);
                        mqThreadPool.execute(task);
                    }

                }
            }
        });




        _bRunning = true;


    }

    /**
     * 停止
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        _bossGroup.shutdownGracefully();
        _workerGroup.shutdownGracefully();

        for (GatherSlot slot : _slots) {
            slot.stop();
        }

        _bRunning = false;
    }

    /**
     * 增加输入端口
     *
     * @param portType 端口类型
     * @param portArgs 端口参数
     * @return
     */
    public GatherSlot addSlot(GatherPortType portType, String portArgs) {
        GatherSlot slot = null;

        switch (portType) {
            case TCP:
                slot = new GatherSlot4TCP(Integer.parseInt(portArgs), this);
                break;
            case UDP:
                slot = new GatherSlot4UDP(Integer.parseInt(portArgs), this);
                break;
            case MQTT:
            default:
                throw new UnsupportedOperationException(portType.name());
        }

        _slots.add(slot);
        return slot;
    }


    EventLoopGroup getBossGroup() {
        return _bossGroup;
    }

    EventLoopGroup getWorkerGroup() {
        return _workerGroup;
    }

    /**
     * 获取 主机名
     *
     * @return name
     */
    public String getName() {
        return name;
    }


    public IBigMQ getBigMQ() {
        return bigMQ;
    }


    public void setBigMQ(IBigMQ bigMQ) {
        this.bigMQ = bigMQ;
    }


    public ExecutorService getMqThreadPool() {
        return mqThreadPool;
    }


    /**
     * 将数据扔到队列中
     * @param packWrap
     */
    public void putToMsgQueue(DataPackWrap packWrap){
        if(null == packWrap){
            return;
        }

        try{
            dataPackQueue.put(packWrap);
        }catch (InterruptedException e){
            //这里一般不会有中断触发这里
            packWrap.getDataPack().freeBuf();
        }


    }


}
