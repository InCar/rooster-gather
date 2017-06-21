package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.mq.IBigMQ;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * 采集槽所在主机
 *
 * @author 熊广化
 */
public class GatherHost {
    private static Logger s_logger = LoggerFactory.getLogger(GatherHost.class);

    /**
     * 主机名
     */
    private String name;

    private EventLoopGroup _bossGroup;
    private EventLoopGroup _workerGroup;

    /**
     * 采集槽列表
     */
    private ArrayList<GatherSlot> _slots = new ArrayList<>();

    /**
     * 数据包任务的管理和执行器
     */
    private DataPackTaskManager dataPackTaskManager;

    /**
     * 操作消息队列接口
     */
    private IBigMQ bigMQ;


    /**
     * 是否已启动
     */
    private Boolean _bRunning = false;

    public GatherHost() {
        this("host" + Calendar.getInstance().getTimeInMillis());
    }

    /**
     * @param name 主机名
     */
    public GatherHost(String name) {
        this.name = name;
        _bossGroup = new NioEventLoopGroup();
        _workerGroup = new NioEventLoopGroup();

        this.dataPackTaskManager = new DataPackTaskManager(this);
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

        dataPackTaskManager.start();

        _bRunning = true;

        s_logger.info(name + "start success!!");
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


    /**
     * 将数据包处理任务扔到队列中
     *
     * @param task
     */
    public void putToCacheQueue(DataPackTask task) {
        if (null == task) {
            return;
        }

        dataPackTaskManager.add(task);
    }


}
