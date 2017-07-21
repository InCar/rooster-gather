package com.incarcloud.rooster.gather.cmd.server;/**
 * Created by fanbeibei on 2017/7/17.
 */

import com.incarcloud.rooster.gather.cmd.CommandServerRespCode;
import com.incarcloud.rooster.gather.cmd.ReqContent;
import com.incarcloud.rooster.gather.cmd.RespContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Fan Beibei
 * @Description: 发送命令业务类
 * @date 2017/7/17 15:09
 */
public class CommandService {
    private static Logger s_logger = LoggerFactory.getLogger(CommandService.class);

    /**
     * 执行命令
     * @param req
     * @return
     */
    public RespContent executeCommand(ReqContent req){

        s_logger.debug(req.toString());

        RespContent resp = new RespContent();
        resp.setCode(CommandServerRespCode.OP_SUCCESS);
        return resp;
    }


}
