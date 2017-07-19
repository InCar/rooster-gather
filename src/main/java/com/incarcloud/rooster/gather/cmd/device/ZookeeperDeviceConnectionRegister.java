package com.incarcloud.rooster.gather.cmd.device;/**
 * Created by fanbeibei on 2017/7/19.
 */

import org.I0Itec.zkclient.ZkClient;

/**
 * @author Fan Beibei
 * @Description: zookeeper实现的注册器
 * @date 2017/7/19 11:43
 */
public class ZookeeperDeviceConnectionRegister implements DeviceConnectionRemoteRegister {
    /**
     * 根路径
     */
    private static  final  String ROOT_PATH = "/rooster-gather";

    private ZkClient zk;


    public ZookeeperDeviceConnectionRegister(String zkSever){
        zk = new ZkClient(zkSever);
        init();
    }

    protected void init(){
        if(!zk.exists(ROOT_PATH)){
            zk.createEphemeral(ROOT_PATH);
        }
    }

    @Override
    public void registerConnection(String vin, String protocal, String serverUrl){
        String nodePath =  ROOT_PATH+"/"+vin;

        if(!zk.exists(nodePath)){
            zk.createPersistent(nodePath,true);
        }
        zk.writeData(nodePath,protocal+","+serverUrl);
    }
}
