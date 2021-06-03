package com.incarcloud.rooster.gather;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.incarcloud.rooster.cache.ICacheManager;
import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.gather.remotecmd.session.SessionFactory;
import com.incarcloud.rooster.mq.RsaActivationMsg;
import com.incarcloud.rooster.security.RsaUtil;
import com.incarcloud.rooster.share.Constants;
import com.incarcloud.rooster.util.GsonFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.time.Instant;
import java.util.*;

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
     * 激活日志
     */
    private static Logger activeLogger = LoggerFactory.getLogger("activeLogger");

    /**
     * 故障日志
     */
    private static Logger faultLogger = LoggerFactory.getLogger("faultLogger");

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

    /**
     * 数据包解析器
     */
    private IDataParser _parser;

    /**
     * 缓存管理器
     */
    private ICacheManager _cacheManager;

    /**
     * @param slot 采集槽
     */
    GatherChannelHandler(GatherSlot slot) {
        _slot = slot;
        _parser = slot.getDataParser();
        _cacheManager = slot.getCacheManager();
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
        // 打印日志
        s_logger.info("Parser Class: {}, Receive Bytes: {}", _parser.getClass(), ByteBufUtil.hexDump(buf));

        // 初始化
        Channel channel = ctx.channel();
        List<DataPack> listPacks;
        Date receiveTime = Date.from(Instant.now()); //接收时间

        try {
            // 1.获得设备号
            String deviceId = _parser.getDeviceId(buf);
            if(StringUtils.isBlank(deviceId)) {
                return;
            }

            // 2.设置平台公钥和私钥数据(限制激活报文和登陆报文)
            int packType = _parser.getPackType(buf);
            // 打印轻量解析前激活报文
            if (Constants.PackType.ACTIVATE == packType) {
                activeLogger.debug("[{}] Receive Active Bytes: {}", GatherChannelHandler.class.getSimpleName(), ByteBufUtil.hexDump(buf));
            }
            // 打印轻量解析前故障报文
            if (Constants.PackType.FAULT == packType) {
                faultLogger.debug("[{}] Receive Fault Bytes: {}", GatherChannelHandler.class.getSimpleName(), ByteBufUtil.hexDump(buf));
            }
            if (Constants.PackType.LOGIN == packType) {
                // 2.1  查询RSA密钥信息
                String rsaPrivateKeyString = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_PRIVATE_KEY_HASH, deviceId);
                String rsaPublicKeyString = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_PUBLIC_KEY_HASH, deviceId);

                // 2.2 打印公钥和私钥
                //s_logger.debug("RSA Private: {}", rsaPrivateKeyString);
                //s_logger.debug("RSA Public Key: {}", rsaPublicKeyString);

                // 2.3 设置公钥和私钥
                if (StringUtils.isNotBlank(rsaPrivateKeyString) && StringUtils.isNotBlank(rsaPublicKeyString)) {
                    // string转map
                    Map<String, String> mapPrivateKey = GsonFactory.newInstance().createGson().fromJson(rsaPrivateKeyString, new TypeToken<Map<String, String>>() {
                    }.getType());
                    Map<String, String> mapPublicKey = GsonFactory.newInstance().createGson().fromJson(rsaPublicKeyString, new TypeToken<Map<String, String>>() {
                    }.getType());

                    // 设置给解析器
                    _parser.setPrivateKey(deviceId, Base64.getDecoder().decode(mapPrivateKey.get(Constants.RSADataMapKey.N)), Base64.getDecoder().decode(mapPrivateKey.get(Constants.RSADataMapKey.E)));
                    _parser.setPublicKey(deviceId, Base64.getDecoder().decode(mapPublicKey.get(Constants.RSADataMapKey.N)), Long.valueOf(mapPublicKey.get(Constants.RSADataMapKey.E)));
                }
            }

            // 3.解析包(分解，校验，解密)
            listPacks = _parser.extract(buf);
            if (null == listPacks || 0 == listPacks.size()) {
                s_logger.info("No packs!!!");
                if (Constants.PackType.ACTIVATE == packType) {
                    activeLogger.info("[{}] No packs!!!", GatherChannelHandler.class.getSimpleName());
                }
                if (Constants.PackType.FAULT == packType) {
                    faultLogger.info("[{}] No packs!!!", GatherChannelHandler.class.getSimpleName());
                }
                return;
            }
            s_logger.info("DataPackList Size: {}", listPacks.size());

            // 4.获得设备报文Meta数据
            metaData = getPackMetaData(listPacks.get(0), _parser);
            s_logger.info("MetaData: {}", metaData);
            if (Constants.PackType.FAULT == packType) {
                // 打印解密之后的故障报文
                faultLogger.debug("[{}] Parse Fault Bytes: {}", GatherChannelHandler.class.getSimpleName(), ByteBufUtil.hexDump(listPacks.get(0).getDataBytes()));
            }
            // 5.激活报文处理
            if(Constants.PackType.ACTIVATE == packType) {
                // 打印解密之后的激活报文
                activeLogger.debug("[{}] Parse Active Bytes: {}", GatherChannelHandler.class.getSimpleName(), ByteBufUtil.hexDump(listPacks.get(0).getDataBytes()));
                // 激活数据包
                String deviceCode = (String) metaData.get(Constants.MetaDataMapKey.DEVICE_SN);
                String vin = (String) metaData.get(Constants.MetaDataMapKey.VIN);
                String adaptedSeries = (String) metaData.get(Constants.MetaDataMapKey.ADAPTED_SERIES_TYPE);

                // 获取缓存中的设备ID
                String cacheDeviceCode = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_SN_HASH, deviceId);

                // 获取缓存中的车架号
                String cacheVin = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH, deviceId);

                // 获取缓存中的设备ID
                String cacheDeviceId = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_VEHICLE_VIN_HASH, vin);

                // 获取缓存中的T-BOX软件包适配车型
                String cacheAdaptedSeries = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_ADAPTED_SERIES_HASH, deviceId);

                // 打印车辆设备激活信息
                s_logger.info("Activate T-Box: deviceId = {}, deviceCode = {}, cacheAdaptedSeries = {}", deviceId, deviceCode, vin, cacheAdaptedSeries);

                // 如果验证失败，不创建RSA公钥信息
                if (StringUtils.isBlank(cacheDeviceCode)) {
                    // 原因一：T-BOX不存在
                    s_logger.info("Activated failed: the device(id={}) is non-exist.", deviceId);
                    activeLogger.info("Activated failed: the device(id={}) is non-exist.", deviceId);

                } else if (!StringUtils.equals(deviceCode, cacheDeviceCode)) {
                    // 原因二：T-BOX的SN与IMEI绑定关系不正确
                    s_logger.info("Activated failed: the device(id={}) mismatches sn.[{}-{}]", deviceId, deviceCode, cacheDeviceCode);
                    activeLogger.info("Activated failed: the device(id={}) mismatches sn.[{}-{}]", deviceId, deviceCode, cacheDeviceCode);

                } else if (StringUtils.isNotBlank(cacheVin) && !StringUtils.equals(vin, cacheVin)) {
                    // 原因三：T-BOX已经绑定车辆
                    s_logger.info("Activated failed: the device(id={}) has been activated.[cacheVin={}]", deviceId, cacheVin);
                    activeLogger.info("Activated failed: the device(id={}) has been activated.[cacheVin={}]", deviceId, cacheVin);

                } else if (null != cacheAdaptedSeries && !StringUtils.equals(adaptedSeries, cacheAdaptedSeries)) {
                    // 原因四：T-BOX软件版本不适配该车系
                    s_logger.info("Activated failed: the device(id={}) is not adapting this series.[{}-{}]", deviceId, adaptedSeries, cacheAdaptedSeries);
                    activeLogger.info("Activated failed: the device(id={}) is not adapting this series.[{}-{}]", deviceId, adaptedSeries, cacheAdaptedSeries);

                } else if (null != cacheDeviceId && !StringUtils.equals(deviceId, cacheDeviceId)){
                    // 原因五：车辆已经绑定T-BOX
                    s_logger.info("Activated failed: the vin (id={}) has been activated.[deviceId={}]", vin, cacheDeviceId);
                    activeLogger.info("Activated failed: the vin (id={}) has been activated.[deviceId={}]", vin, cacheDeviceId);

                }else {
                    // 5.1 临时创建一份新RSA密钥，存储到Redis
                    RsaUtil.RsaEntity rsaEntity = RsaUtil.generateRsaEntity();
                    RsaActivationMsg activationMsg = new RsaActivationMsg();
                    activationMsg.setDeviceId(deviceId);
                    activationMsg.setRsaEntity(rsaEntity);

                    // 5.2 缓存到缓存器(公钥+私钥)
                    Map<String, String> privateKeyMap = new HashMap<>();
                    privateKeyMap.put(Constants.RSADataMapKey.N, activationMsg.getRsaPrivateModulus());
                    privateKeyMap.put(Constants.RSADataMapKey.E, activationMsg.getRsaPrivateExponent());

                    Map<String, String> publicKeyMap = new HashMap<>();
                    publicKeyMap.put(Constants.RSADataMapKey.N, activationMsg.getRsaPublicModulus());
                    publicKeyMap.put(Constants.RSADataMapKey.E, String.valueOf(activationMsg.getRsaPublicExponent()));

                    _cacheManager.hset(Constants.CacheNamespaceKey.CACHE_DEVICE_PRIVATE_KEY_HASH, deviceId, GsonFactory.newInstance().createGson().toJson(privateKeyMap));
                    _cacheManager.hset(Constants.CacheNamespaceKey.CACHE_DEVICE_PUBLIC_KEY_HASH, deviceId, GsonFactory.newInstance().createGson().toJson(publicKeyMap));

                    // 5.3 设置给解析器
                    _parser.setPrivateKey(deviceId, rsaEntity.getPrivateKeyModulusBytes(), rsaEntity.getPrivateKeyExponentBytes());
                    _parser.setPublicKey(deviceId, rsaEntity.getPublicKeyModulusBytes(), rsaEntity.getPublicKeyExponent());

                    // 5.4 发送MQ消息交给Transfer继续处理
                    activationMsg.setDeviceCode(deviceCode);
                    activationMsg.setVin(vin);

                    activeLogger.debug("[{}] Send active msg to transfer: {}", GatherChannelHandler.class.getSimpleName(), GsonFactory.newInstance().createGson().toJson(activationMsg));
                    _slot.putToActivationMsgToMQ(activationMsg);
                }
            }

            // 6.设置SecurityKey缓存(限登陆报文)
            if (Constants.PackType.LOGIN == packType) {
                // 如果登录报文的VIN和激活报文VIN不一致
                String vin = (String) metaData.get(Constants.MetaDataMapKey.VIN);
                String cacheVin = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH, deviceId);
                if(StringUtils.isNotBlank(cacheVin) && !StringUtils.equals(cacheVin, vin)) {
                    // 主动断开客户端连接
                    channel.close();
                    // 打印异常日志
                    s_logger.info("Activated T-BOX({}) for vin({}) used illegal login info(errorVin = {}).", deviceId, cacheVin, vin);
                    return;
                }

                //校验车辆绑定的deviceId
                String cacheDeviceId = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_VEHICLE_VIN_HASH, vin);
                //非第一次登录： VIN码->设备 关联关系与缓存不符
                if (null != cacheDeviceId && !StringUtils.equals(deviceId, cacheDeviceId)){
                    // 主动断开客户端连接
                    channel.close();
                    // 打印异常日志
                    s_logger.info("Activated vin({}) for deviceId({}) used illegal login info(errorDeviceId = {})", vin, cacheDeviceId,deviceId);

                }

                // 缓存动态密钥，用于构建远程命令报文
                byte[] securityKeyBytes = _parser.getSecurityKey(deviceId);
                // 新增ASE加密CBC模式偏移量
                byte[] securityKeyOffsetBytes = _parser.getSecurityKeyOffset(deviceId);
                if (null != securityKeyBytes) {
                    // 存储结构：{"s": "", "p": ""} --> s: ASE密钥, p: 偏移量
                    Map<String, String> securityKeyMap = new HashMap<>();
                    securityKeyMap.put(Constants.AESDataMapKey.S, Base64.getEncoder().encodeToString(securityKeyBytes));
                    // 判断AES加密是否有偏移量
                    if (null != securityKeyOffsetBytes) {
                        securityKeyMap.put(Constants.AESDataMapKey.P, Base64.getEncoder().encodeToString(securityKeyOffsetBytes));
                    }

                    // 存储到缓存服务器
                    _cacheManager.hset(Constants.CacheNamespaceKey.CACHE_DEVICE_SECURITY_KEY_HASH, deviceId, GsonFactory.newInstance().createGson().toJson(securityKeyMap));
                }

                // 设备只能被激活一次
                //String cacheDeviceId = _cacheManager.hget(Constants.CacheNamespaceKey.CACHE_VEHICLE_VIN_HASH, vin);
                if (StringUtils.isBlank(cacheDeviceId)) {
                    // 第一次登录成功说明激活成功，维护车架号与设备号的关系
                    if (StringUtils.isNotBlank(cacheVin) && StringUtils.equals(cacheVin, vin)) {
                        _cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_VIN_HASH, cacheVin, deviceId);
                        s_logger.info("Normal login first, activated success: deviceId = {}, vin = {}", deviceId, vin);
                        activeLogger.info("[{}] Normal login first, activated success: deviceId = {}, vin = {}", GatherChannelHandler.class.getSimpleName(), deviceId, vin);
                    }
                }
            }

            // 7.扔到host的消息队列
            for (DataPack pack : listPacks) {
                // 填充接收时间
                pack.setReceiveTime(receiveTime); //数据包的接收时间

                // 处理消息队列
                DataPackWrap dpw = new DataPackWrap(channel, _parser, pack, metaData);

                // 放到缓存队列
                _slot.putToCacheQueue(dpw);
                s_logger.debug("Put ({}) to queue ok!", pack);
            }

            //　8.缓存deviceId - Channel 关系
            if (StringUtils.isNotBlank(deviceId)) {
                SessionFactory.getInstance().createRelationSessionId(ctx, deviceId);
            }

        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error(e.getMessage());
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        SocketAddress address = ctx.channel().remoteAddress();

        s_logger.info("Device({}) connected!", address);
        super.channelActive(ctx);
    }

    /**
     * 客户端主动断开
     *
     * @param ctx 　网络通道
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
     * @param parser   解析器
     * @return
     */
    private Map<String, Object> getPackMetaData(DataPack dataPack, IDataParser parser) {
        if (null == dataPack || null == dataPack.getDataBytes() || null == parser) {
            return null;
        }
        ByteBuf buffer = Unpooled.wrappedBuffer(dataPack.getDataBytes());
        Map<String, Object> mapMeta = parser.getMetaData(buffer);
        ReferenceCountUtil.release(buffer);
        return mapMeta;
    }
}