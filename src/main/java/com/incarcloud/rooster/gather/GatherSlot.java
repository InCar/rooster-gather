package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.mq.MQException;
import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InvalidClassException;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 采集处理槽父类
 * </p>
 * 
 * 一个处理槽有一个采集端口，一个包解析器，结果输出给MQ
 * 
 * @author 熊广化
 *
 */
public abstract class GatherSlot {
	private static Logger s_logger = LoggerFactory.getLogger(GatherSlot.class);
	
	
	/**
	 * 名称
	 */
	private String name;

	/**
	 * 采集槽所在主机
	 */
	private GatherHost _host;
	/**
	 * 
	 */
	private IDataParser _dataParser;

	/**
	 * @param host
	 *            采集槽所在主机
	 */
	GatherSlot(GatherHost host) {
		_host = host;
		this.name = _host.getName()+"-"+"slot"+ new Date().getTime();
	}

	/**
	 * @param name 采集槽名称
	 * @param _host 采集槽所在主机
	 */
	public GatherSlot(String name, GatherHost _host) {
		this.name = name;
		this._host = _host;
	}

	public void setDataParser(String parser)
			throws InvalidClassException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		setDataParser(parser, "com.incarcloud.rooster.datapack");
	}

	/**
	 * 设置数据解析器类
	 * 
	 * @param parser
	 *            类名
	 * @param pack
	 *            类所在包名
	 * @throws InvalidClassException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void setDataParser(String parser, String pack)
			throws InvalidClassException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		// 利用反射构造出对应的解析器对象
		String fullName = String.format("%s.%s", pack, parser);
		Class<?> ParserClass = Class.forName(fullName);
		IDataParser dataParser = (IDataParser) ParserClass.newInstance();
		if (dataParser == null)
			throw new InvalidClassException(
					String.format("%s does not implement interface %s", fullName, IDataParser.class.toString()));

		_dataParser = dataParser;
	}

	IDataParser getDataParser() {
		return _dataParser;
	}

	/**
	 * 开始运行
	 */
	abstract void start();

	/**
	 * 停止
	 */
	abstract void stop();

	/**
	 * 投递数据到MQ
	 * 
	 * 
	 * @param listPacks
	 *            投递的数据
	 * @return
	 */
	@Deprecated
	Promise<Object, List<MQException>, Object> postMQ(List<DataPack> listPacks) {
		return _host.postMQ(listPacks);
	}

	/**
	 * 获取 采集主机
	 * 
	 * @return _host
	 */
	public GatherHost get_host() {
		return _host;
	}

	/** 
	 * 获取 名称 
	 * @return name 
	 */
	public String getName() {
		return name;
	}
	

}