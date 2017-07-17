package com.incarcloud.rooster.gather.cmd;/**
 * Created by fanbeibei on 2017/7/17.
 */

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Fan Beibei
 * @Description: 设备连接缓存
 * @date 2017/7/17 10:54
 */
public class DeviceConnectionCache implements DeviceConnectionContainer{

    /**
     * vin -> DeviceConnection
     */
    private ConcurrentHashMap<String,DeviceConnection> connMap = new ConcurrentHashMap<>();

    public  DeviceConnectionCache(){

    }

    /**
     * 添加设备连接
     *
     * @param conn
     * @return
     */
    public void addDeviceConnection(DeviceConnection conn) throws Exception{
        if(null == conn){
            throw new IllegalArgumentException();
        }

        connMap.put(conn.getVin(),conn);
    }

    /**
     * 移除设备连接
     *
     * @param vin
     *            车辆vin码
     * @return
     */
    public void removeDeviceConnection(String vin) throws Exception{
        connMap.remove(vin);
    }

    /**
     * 获取设备连接
     *
     * @param vin
     *            车辆vin码
     * @return
     * @throws Exception
     */
    public DeviceConnection getDeviceConnection(String vin) throws Exception{
        return connMap.get(vin);
    }
}
