package com.incarcloud.rooster.gather.cmd.server;/**
 * Created by fanbeibei on 2017/7/17.
 */

import com.incarcloud.rooster.datapack.CommandFacotry;
import com.incarcloud.rooster.datapack.CommandFacotryManager;
import com.incarcloud.rooster.gather.cmd.CommandServerRespCode;
import com.incarcloud.rooster.gather.cmd.ReqContent;
import com.incarcloud.rooster.gather.cmd.RespContent;
import com.incarcloud.rooster.gather.cmd.device.DeviceConnection;
import com.incarcloud.rooster.gather.cmd.device.DeviceConnectionContainer;
import com.incarcloud.rooster.util.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Fan Beibei
 * @Description: 发送命令业务类
 * @date 2017/7/17 15:09
 */
public class CommandService {
    private static Logger s_logger = LoggerFactory.getLogger(CommandService.class);

    private DeviceConnectionContainer connContainer;
//    private CommandFacotry commandFacotry;

    public CommandService(DeviceConnectionContainer connContainer/*, CommandFacotry commandFacotry*/) {
        this.connContainer = connContainer;
//        this.commandFacotry = commandFacotry;
    }

    /**
     * 执行命令
     *
     * @param req
     * @return
     */
    public RespContent executeCommand(ReqContent req) {
        RespContent resp = new RespContent();
        if (null == req || null == req.getCmdType() || StringUtil.isBlank(req.getVin())) {
            resp.setCode(CommandServerRespCode.REQ_PARAM_ERROR);
            return resp;
        }

        s_logger.debug(req.toString());

        DeviceConnection conn = connContainer.getDeviceConnection(req.getVin());
        if (null == conn || !conn.getChannel().isActive()) {
            resp.setCode(CommandServerRespCode.DEV_OFFLINE);
            s_logger.info("device offline ,vin=" + req.getVin() + ",CmdType=" + req.getCmdType());
            return resp;
        }

        try {
            CommandFacotry commandFacotry = CommandFacotryManager.getCommandFacotry(conn.getProtocol());
            ByteBuf cmdBuf = commandFacotry.createCommand(req.getCmdType());
            if (null == cmdBuf) {
                resp.setCode(CommandServerRespCode.OP_NOTSUPPORT);
                s_logger.error("command not supoort,vin=" + req.getVin() + ",CmdType=" + req.getCmdType());
            } else {
                s_logger.debug(conn.getChannel().isActive() + "");
                conn.getChannel().writeAndFlush(cmdBuf);
                resp.setCode(CommandServerRespCode.OP_SUCCESS);
                s_logger.debug("command execute success ,vin=" + req.getVin() + ",CmdType=" + req.getCmdType());
            }
        } catch (Exception e) {
            e.printStackTrace();
            resp.setCode(CommandServerRespCode.OTHER_ERROR);
            resp.setDescInfo(e.getMessage());
            s_logger.error("command excepiton,vin=" + req.getVin() + ",CmdType=" + req.getCmdType() + "," + e.getMessage());
        }

        return resp;
    }


}
