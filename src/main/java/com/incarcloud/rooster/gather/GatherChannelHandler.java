package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.gather.remotecmd.session.SessionFactory;
import com.incarcloud.rooster.share.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 采集器的通道处理类
 *
 * @author 熊广化
 */
public class GatherChannelHandler extends ChannelInboundHandlerAdapter {

    /**
     * Logger
     */
    private static Logger s_logger = LoggerFactory.getLogger(GatherChannelHandler.class);

    /**
     * 设备报文Meta数据
     */
    private Map<String, Object> metaData;

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

        s_logger.debug("Buf Size: {}", buf.readableBytes());

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
        //TCP连接时，创建缓存信息
        SessionFactory.getInstance().createSession(ctx);
        _buffer = ctx.alloc().buffer();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        //断开清除连接缓存
        SessionFactory.getInstance().cancelSession(ctx);
        _buffer.release();
        _buffer = null;
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        s_logger.error("{}", cause.toString());
        ctx.close();
    }

    private void OnRead(ChannelHandlerContext ctx, ByteBuf buf) {
        s_logger.info("Receive Bytes: {}", ByteBufUtil.hexDump(buf));
        s_logger.debug("Parser Class: {}", _parser.getClass());

        Channel channel = ctx.channel();
        List<DataPack> listPacks = null;
        try {

            // 1、解析包(分解，校验，解密)
            listPacks = _parser.extract(buf);
            s_logger.debug("DataPackList Size: {}", listPacks.size());

            if (null == listPacks || 0 == listPacks.size()) {
                s_logger.debug("no packs!!!");
                return;
            }

            // 2、获得设备报文Meta数据
            metaData = getPackMetaData(listPacks.get(0), _parser);
            s_logger.debug("MetaData: {}", metaData);

            // 3、扔到host的消息队列
            Date currentTime = Calendar.getInstance().getTime();
            for (DataPack pack : listPacks) {
                // 填充接收时间
                pack.setReceiveTime(currentTime);//数据包的接收时间

                // 处理消息队列
                DataPackWrap dpw = new DataPackWrap(channel, _parser, pack, metaData);

                // 放到缓存队列
                _slot.putToCacheQueue(dpw);
                s_logger.debug("Put ({}) to queue ok!", pack);
            }

            //　4、缓存deviceId - Channel 关系
            String deviceId = (String) metaData.get(Constants.MetaDataMapKey.DEVICE_ID);
            if (!StringUtils.isBlank(deviceId)) {
                SessionFactory.getInstance().createRelationSessionId(ctx, deviceId);
            }

        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error(e.getMessage());
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        SocketAddress devAddr = ctx.channel().remoteAddress();

        s_logger.info("Device({}) connected!", devAddr);
        super.channelActive(ctx);
    }

    /**
     * 客户端主动断开
     *
     * @param ctx　网络通道
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 打印断开连接信息
        s_logger.info("Device({}) disconnected!", ctx.channel().remoteAddress());
    }

    /**
     * 获取vin/设备号/协议
     *
     * @param dataPack 数据包
     * @param parser 解析器
     * @return
     */
    private Map<String, Object> getPackMetaData(DataPack dataPack, IDataParser parser) {
        if(null == dataPack || null == dataPack || null != dataPack.getDataBytes()) {
            return null;
        }
        ByteBuf buf = Unpooled.wrappedBuffer(dataPack.getDataBytes());
        return parser.getMetaData(buf);
    }
}