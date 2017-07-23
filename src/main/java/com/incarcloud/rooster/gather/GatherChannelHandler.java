package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.gather.cmd.device.DeviceConnection;
import com.incarcloud.rooster.util.StringUtil;
import org.slf4j.LoggerFactory;

import com.incarcloud.rooster.datapack.IDataParser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.net.SocketAddress;
import java.util.List;

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
            String v=null;//TODO 这里和下面待修改
            if (null == vin) {//已注册就不用再次注册
                v = getVin(buf,_parser);
            }

            // 1、解析包
            listPacks = _parser.extract(buf);

            if (null == listPacks || 0 == listPacks.size()) {
                s_logger.debug("no packs!!");
                return;
            }

            //TODO
            if (null == vin) {//已注册就不用再次注册
                registerConnection(v, ctx.channel(),listPacks.get(0).getProtocol());//TODO
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
     * 注册设备连接
     *
     * @param vin
     * @param channel
     * @param  protocol
     */
    private void registerConnection(String vin, Channel channel,String protocol) {

        if (StringUtil.isBlank(vin)) {
            return;
        }

        //1.缓存连接
        DeviceConnection conn = new DeviceConnection(vin, channel,protocol);
        _slot.getDeviceConnectionContainer().addDeviceConnection(conn);


        //2.远程注册,开线程避免阻塞
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    _slot.registerConnectionToRemote(conn);
                    GatherChannelHandler.this.vin = vin;
                    s_logger.debug("success register device connection to remote,vin="+vin);
                }catch (Exception e){
                    e.printStackTrace();
                    s_logger.error(e.getMessage());
                }
            }
        }).start();

    }


    /**
     * 获取vin码
     * @param buf
     * @param _parser
     * @return
     */
    private String getVin(ByteBuf buf,IDataParser _parser){
        return "123456789";
    }
}