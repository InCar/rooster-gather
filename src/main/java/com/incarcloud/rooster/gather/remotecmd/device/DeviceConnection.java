package com.incarcloud.rooster.gather.remotecmd.device;

import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;

/**
 * 设备连接
 * 
 * @author fanbeibei
 *
 */
public class DeviceConnection {

	/**
	 * 车辆vin码
	 */
	private String vin;
	/**
	 * 通讯通道
	 */
	private Channel channel;

	/**
	 * 设备协议
	 */
	private String protocol;


	public DeviceConnection(String vin, Channel channel,String protocol) {
		if(StringUtils.isBlank(vin) || null == channel || StringUtils.isBlank(protocol)){
			throw new IllegalArgumentException();
		}

		this.vin = vin;
		this.channel = channel;
		this.protocol = protocol;
	}

	public String getVin() {
		return vin;
	}

	public Channel getChannel() {
		return channel;
	}

	public String getProtocol() {
		return protocol;
	}
}
