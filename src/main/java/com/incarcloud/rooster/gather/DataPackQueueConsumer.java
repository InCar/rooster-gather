package com.incarcloud.rooster.gather;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * @author Fan Beibei
 * @ClassName: DataPackQueueConsumer
 * @Description: 数据包队列的消费者
 * @date 2017-06-07 17:34
 */
public class DataPackQueueConsumer {
    private static Logger s_logger = LoggerFactory.getLogger(DataPackQueueConsumer.class);
    /**
     * 默认线程数
     */
    private static final int DEFAULT_THREAD_COUNT = 20;


    /**
     * 要消费的队列
     */
    private BlockingQueue<DataPackTask> targetQueue;

    /**
     * 执行消费的线程组
     */
    private ThreadGroup threadGroup;

    /**
     * 消费线程数
     */
    private int threadCount = DEFAULT_THREAD_COUNT;

    /**
     * 所属主机
     */
    private GatherHost host;


    /**
     * @param host        所属主机
     * @param targetQueue 要消费的队列
     */
    public DataPackQueueConsumer(GatherHost host, BlockingQueue<DataPackTask> targetQueue) {
        if (null == targetQueue) {
            throw new IllegalArgumentException();
        }
        this.host = host;
        this.targetQueue = targetQueue;
        this.threadGroup = new ThreadGroup(host.getName() + "-DataPackQueueConsumer-ThreadGroup");
    }

    /**
     * @param host        所属主机
     * @param targetQueue 要消费的队列
     * @param threadCount 消费线程数
     */
    public DataPackQueueConsumer(GatherHost host, BlockingQueue<DataPackTask> targetQueue, int threadCount) {
        if (null == targetQueue) {
            throw new IllegalArgumentException();
        }
        this.host = host;
        this.targetQueue = targetQueue;
        this.threadGroup = new ThreadGroup(host.getName() + "-DataPackQueueConsumer-ThreadGroup");
        this.threadCount = threadCount;

    }


    /**
     * 启动
     */
    public void start() {
        for (int i = 0; i < threadCount; i++) {
            new Thread(threadGroup, new QueueConsumerThread()).start();
        }


    }



    /**
     * @author Fan Beibei
     * @ClassName: QueueConsumerThread
     * @Description: 取发送任务并执行的线程
     * @date 2017-06-07 17:34
     */
    private class QueueConsumerThread implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    DataPackTask packTask = targetQueue.take();//可能阻塞
                    packTask.sendDataPackToMQ(host.getBigMQ());
                }
            } catch (InterruptedException e) {

            }


        }
    }

}
