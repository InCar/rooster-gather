package com.incarcloud.rooster.gather;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Fan Beibei
 * @Description: 数据包任务的管理和执行器
 * @date 2017-06-07 17:34
 */
public class DataPackTaskManager {
    private static Logger s_logger = LoggerFactory.getLogger(DataPackTaskManager.class);




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
    private BlockingQueue<DataPackTask> cacheQueue;
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
    public DataPackTaskManager(GatherHost host) {
        this(host, DEFULT_CACHE_QUEUE_SIZE, DEFAULT_THREAD_COUNT);
    }

    /**
     * @param host           所属主机
     * @param cacheQueueSize 缓存队列大小
     * @param threadCount    执行任务线程数
     */
    public DataPackTaskManager(GatherHost host, int cacheQueueSize, int threadCount) {
        if (null == host || cacheQueueSize <= 0 || threadCount <= 0)
            throw new IllegalArgumentException();

        this.host = host;
        this.threadGroup = new ThreadGroup(host.getName() + "-DataPackTaskManager-ThreadGroup");

        this.name = host.getName() + "-DataPackTaskManager";

        this.cacheQueue = new ArrayBlockingQueue<DataPackTask>(cacheQueueSize);
        this.threadCount = threadCount;

    }


    /**
     * 添加任务 （线程安全）
     *
     * @param task
     */
    public void add(DataPackTask task) {
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
            new Thread(threadGroup, new QueueConsumerThread()).start();
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

                s_logger.info(DataPackTaskManager.this.name + " queue  contains  "
                        + remainQueueCount + " datapack for  send,last " + period + " second " + newInQueueCount
                        + " in queue, " + newOutQueueCount + "  out  queue!");


            }
        }, period, period, TimeUnit.SECONDS);
    }


    /**
     * @author Fan Beibei
     * @Description: 取 发送任务 并执行的线程
     * @date 2017-06-07 17:34
     */
    private class QueueConsumerThread implements Runnable {
        @Override
        public void run() {

            while (true) {
                try {
                    DataPackTask packTask = cacheQueue.take();//可能阻塞
                    outQueueCount.incrementAndGet();
                    packTask.sendDataPackToMQ(host.getBigMQ());
                } catch (InterruptedException e) {

                }
            }


        }
    }

}
