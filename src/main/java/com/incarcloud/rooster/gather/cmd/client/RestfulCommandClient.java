package com.incarcloud.rooster.gather.cmd.client;/**
 * Created by fanbeibei on 2017/7/19.
 */

import com.google.gson.Gson;
import com.incarcloud.rooster.gather.cmd.CommandType;
import com.incarcloud.rooster.gather.cmd.ReqContent;
import com.incarcloud.rooster.gather.cmd.RespContent;
import com.incarcloud.rooster.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Fan Beibei
 * @Description: restful客户端
 * @date 2017/7/19 17:02
 */
public  abstract class RestfulCommandClient extends AbstractCommandClient {
    private static Logger s_logger = LoggerFactory.getLogger(RestfulCommandClient.class);

    /**
     * @param url     服务端地址
     * @param vin     车辆vin码
     * @param command 指令类型
     * @throws Exception
     */
    @Override
    public RespContent sendCommand(String url, String vin, CommandType command) throws Exception {
        if (StringUtil.isBlank(url) || StringUtil.isBlank(vin) || null == command) {
            throw new IllegalArgumentException();
        }

        ReqContent req = new ReqContent(command, vin);

        Gson gson = new Gson();
        String result = HttpClientUtil.postJson(url, gson.toJson(req));

        RespContent resp = gson.fromJson(result, RespContent.class);

        return resp;


    }


}
