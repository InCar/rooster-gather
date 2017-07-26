package com.incarcloud.rooster.gather;/**
 * Created by fanbeibei on 2017/7/20.
 */

import com.incarcloud.rooster.gather.cmd.CommandType;
import com.incarcloud.rooster.gather.cmd.RespContent;
import com.incarcloud.rooster.gather.cmd.client.CommandClient;

/**
 * @author Fan Beibei
 * @Description: 描述
 * @date 2017/7/20 11:07
 */
public class RestfulCommandClientTest {
//    @Test
    public void testSend(){
        CommandClient client = new ZookeeperRestfulCommandClient("127.0.0.1:2181");
        try {
            RespContent resp =  client.sendCommand("123456789", CommandType.FLASH_LIGHTS_ON);

            System.out.println(resp);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
