package com.incarcloud.rooster.gather.remotecmd.session;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Kong on 2018/1/8.
 */
public class SessionFactory {

    /**
     * sessionMap
     * key : sessionId
     * value : Session
     */
    private ConcurrentMap<String,Session> sessionMap = new ConcurrentHashMap<>() ;

    /**
     * iMeiToSessionId
     * key : deviceId
     * value : sessionId
     */
    private ConcurrentMap<String,String> deviceIdToSessionId = new ConcurrentHashMap<>() ;

    /**
     * iMeiToSessionId
     * key : deviceId
     * value : sessionId
     */
    private ConcurrentMap<String,String> sessionIdToDeviceId = new ConcurrentHashMap<>() ;

    private static SessionFactory sessionFactory;

    private SessionFactory() {
    }

    public static SessionFactory getInstance() {
        if (sessionFactory == null) {
            sessionFactory = new SessionFactory();
        }
        return sessionFactory;
    }

    public void createSession(ChannelHandlerContext ctx) {
        Session session = new Session(ctx.channel().remoteAddress().toString(), ctx);
        sessionMap.put(ctx.channel().remoteAddress().toString(), session);
    }

    public void createRelationSessionId(ChannelHandlerContext ctx,String deviceId){
        String sessionId = ctx.channel().remoteAddress().toString() ;
        deviceIdToSessionId.put(deviceId,sessionId) ;
        sessionIdToDeviceId.put(sessionId,deviceId) ;
    }

    /**
     * 清除缓存信息
     * @param ctx
     */
    public void cancelSession(ChannelHandlerContext ctx){
        String sessionId = ctx.channel().remoteAddress().toString() ;
        String deviceId = sessionIdToDeviceId.get(sessionId) ;
        sessionMap.remove(sessionId) ;
        deviceIdToSessionId.remove(deviceId) ;
        sessionIdToDeviceId.remove(sessionId) ;
    }

    public String getSessionId(String deviceId){
        return deviceIdToSessionId.get(deviceId) ;
    }

    public Session getSession(String sessionId) {
        return sessionMap.get(sessionId);
    }

    public Session getSession(ChannelHandlerContext ctx) {
        return sessionMap.get(ctx.channel().remoteAddress().toString());
    }
}
