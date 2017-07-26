package com.incarcloud.rooster.gather.conf;/**
 * Created by fanbeibei on 2017/7/12.
 */

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author Fan Beibei
 * @Description: 描述
 * @date 2017/7/12 17:03
 */
@Component
public class HostConfig {
    /**
     * slot配置信息，格式如下
     *      解析器标识:协议名:端口,解析器标识:协议名:端口
     *
     *
     */
    private String slots;
    /**
     * restful接口的监听端口，用于向设备下发命令
     */
    private int restfulPort;
    /**
     * 注册中心
     */
    private String registerCenter;

    @Value("${rooster.host.registerCenter}")
    public void setRegisterCenter(String registerCenter) {
        this.registerCenter = registerCenter;
    }

    public String getRegisterCenter() {
        return registerCenter;
    }

    @Value("${rooster.host.restfulPort}")
    public void setRestfulPort(int restfulPort) {
        this.restfulPort = restfulPort;
    }

    public int getRestfulPort() {
        return restfulPort;
    }

    @Value("${rooster.host.slots}")
    public void setSlots(String slots) {
        this.slots = slots;
    }

    public String getSlots() {
        return slots;
    }
}
