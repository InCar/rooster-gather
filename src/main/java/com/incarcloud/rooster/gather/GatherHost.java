package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 采集槽所在主机
 * 
 * @author 熊广化
 *
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
	 * 是否已启动
	 */
	private Boolean _bRunning = false;

	public GatherHost() {
		this.name = "host"+new Date().getTime();
		_bossGroup = new NioEventLoopGroup();
		_workerGroup = new NioEventLoopGroup();
	}
	

	/**
	 * @param name 主机名
	 */
	public GatherHost(String name) {
		this();
		this.name = name;
	}


	/**
	 * 启动
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		if (_bRunning)
			return;

		for (GatherSlot slot : _slots) {
			slot.start();
		}
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
	 * @param portType
	 *            端口类型
	 * @param portArgs
	 *            端口参数
	 * 
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

	/**
	 * 投递数据包到MQ
	 * 
	 * @param listPacks
	 *            投递的数据包
	 * @return
	 */
	@Deprecated
	Promise<Object, List<MQException>, Object> postMQ(List<DataPack> listPacks) {
		// TODO: 多线程环境,需要尽可能快速的处理
		// 先暂存在本地的内存上,然后再发送
		IBigMQ mq = null;

		DeferredObject<Object, List<MQException>, Object> d = new DeferredObject<>();
		return d.promise();
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

}
