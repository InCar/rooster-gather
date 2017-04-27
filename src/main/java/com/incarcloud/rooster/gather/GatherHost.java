package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

// 采集宿主
public class GatherHost {
    private static Logger s_logger = LoggerFactory.getLogger(GatherHost.class);
    private EventLoopGroup _bossGroup;
    private EventLoopGroup _workerGroup;
    private ArrayList<GatherSlot> _slots = new ArrayList<>();
    private Boolean _bRunning = false;

    public GatherHost(){
        _bossGroup = new NioEventLoopGroup();
        _workerGroup = new NioEventLoopGroup();
    }

    // 启动
    public void start() throws Exception{
    	if(_bRunning) return;
    	
        for(GatherSlot slot: _slots){
            slot.start();
        }
        _bRunning = true;
    }

    // 停止
    public void stop()throws Exception{
        _bossGroup.shutdownGracefully();
        _workerGroup.shutdownGracefully();

        for(GatherSlot slot: _slots){
            slot.stop();
        }

        _bRunning = false;
    }

    // 增加输入端口
    public GatherSlot addSlot(GatherPortType portType, String portArgs){
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

    // 投递到MQ
    Promise<Object,List<MQException>, Object> postMQ(List<DataPack> listPacks){
        // TODO: 多线程环境,需要尽可能快速的处理
        // 先暂存在本地的内存上,然后再发送
        IBigMQ mq = null;

        DeferredObject<Object,List<MQException>, Object> d = new DeferredObject<>();
        return d.promise();
    }

    EventLoopGroup getBossGroup(){ return _bossGroup;}
    EventLoopGroup getWorkerGroup(){ return _workerGroup; }
}
