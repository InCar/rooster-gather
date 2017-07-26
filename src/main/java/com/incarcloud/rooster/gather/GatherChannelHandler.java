package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.gather.remotecmd.device.DeviceConnection;
import com.incarcloud.rooster.util.StringUtil;
import org.slf4j.LoggerFactory;

import com.incarcloud.rooster.datapack.IDataParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

/**
 * 采集器的通道处理类
 *
 * @author 熊广化
 */
public class GatherChannelHandler extends ChannelInboundHandlerAdapter {
    private static org.slf4j.Logger s_logger = LoggerFactory.getLogger(GatherChannelHandler.class);


    private String vin;

    /**
     * 所属的采集槽
     */
    private GatherSlot _slot;

    /**
     * 累积缓冲区
     */
    private ByteBuf _buffer = null;
    private IDataParser _parser = null;

    /**
     * @param slot 采集槽
     */
    GatherChannelHandler(GatherSlot slot) {
        _slot = slot;
        _parser = slot.getDataParser();

    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;

        if (buf.readableBytes() > 2 * 1024 * 1024) { //大于2M 直接丢弃
            buf.release();
            return;
        }


        _buffer.writeBytes(buf);
        buf.release();

        this.OnRead(ctx, _buffer);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        _buffer = ctx.alloc().buffer();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        _buffer.release();
        _buffer = null;
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        s_logger.error("{}", cause.toString());
        ctx.close();
    }

    private void OnRead(ChannelHandlerContext ctx, ByteBuf buf) {
//        s_logger.debug("!!!!----" + _parser.getClass());

        Channel channel = ctx.channel();
        List<DataPack> listPacks = null;
        try {

            //注册设备会话
            if (null == vin) {//已注册就不用再次注册
                Map<String,Object> metaData = getPackMetaData(buf,_parser);
                registerConnection(metaData,channel);
            }

            // 1、解析包
            listPacks = _parser.extract(buf);

            if (null == listPacks || 0 == listPacks.size()) {
                s_logger.debug("no packs!!");
                return;
            }


            // 2、扔到host的消息队列
            for (DataPack pack : listPacks) {
                _slot.putToCacheQueue(new DataPackWrap(channel, _parser, pack));
//                s_logger.debug("#####putToCacheQueue OK");
            }

        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error(e.getMessage());
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        SocketAddress devAddr = ctx.channel().remoteAddress();

        s_logger.info("device " + devAddr + " connected");
        super.channelActive(ctx);
    }

    /**
     * 客户端主动断开
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        SocketAddress devAddr = ctx.channel().remoteAddress();
        s_logger.info("device " + devAddr + " disconnected");

        if (null != vin) {//释放掉缓存的连接
            _slot.getDeviceConnectionContainer().removeDeviceConnection(vin);
            _slot.removeConnectionFromRemote(vin);
            s_logger.debug("success remove device connection from remote,vin="+vin);
        }
    }


    /**
     * 注册设备连接,便于下发命令
     *
     * @param metaData 包含车辆 vin/设备号/协议
     * @param channel
     */
    private void registerConnection(Map<String,Object> metaData,Channel channel) {
        String vin0 = (String) metaData.get("vin");
        String protocol = (String) metaData.get("protocol");


        s_logger.debug("registerConnection vin :"+vin0);

        if (StringUtil.isBlank(vin0)) {//无vin码的连接不注册
            return;


            /* //TODO 不支持无vin码的协议
            String deviceId = (String) metaData.get("deviceId");
            if(StringUtil.isBlank(deviceId)){
                s_logger.error("vin  and deviceId are all null !!");
                return;
            }

            //没有vin码就用设备号加协议(不带版本号)代替
            String protocolWithOutVersion =  protocol.split("\\-")[0]+"-"+protocol.split("\\-")[1];
            vin0 = protocolWithOutVersion+"_"+deviceId;
            */
        }



        //1.缓存连接
        DeviceConnection conn = new DeviceConnection(vin0, channel,protocol);
        _slot.getDeviceConnectionContainer().addDeviceConnection(conn);
        final String _vin0 = vin0;

        //2.远程注册,开线程避免阻塞
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    _slot.registerConnectionToRemote(conn);
                    GatherChannelHandler.this.vin = _vin0;
                    s_logger.debug("success register device connection to remote,vin="+vin);
                }catch (Exception e){
                    e.printStackTrace();
                    s_logger.error(e.getMessage());
                }
            }
        }).start();

    }


    /**
     * 获取vin/设备号/协议
     * @param buf
     * @param _parser
     * @return
     */
    private Map<String,Object> getPackMetaData(ByteBuf buf, IDataParser _parser){
        return _parser.getMetaData(buf);
    }
}