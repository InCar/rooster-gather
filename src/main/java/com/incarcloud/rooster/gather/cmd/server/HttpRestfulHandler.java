package com.incarcloud.rooster.gather.cmd.server;/**
 * Created by fanbeibei on 2017/7/17.
 */

import com.google.gson.Gson;
import com.incarcloud.rooster.gather.cmd.CommandServerRespCode;
import com.incarcloud.rooster.gather.cmd.ReqContent;
import com.incarcloud.rooster.gather.cmd.RespContent;
import com.incarcloud.rooster.util.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;

/**
 * @author Fan Beibei
 * @Description: http  restful 处理,仅支持 application/json
 * @date 2017/7/17 14:26
 */
public class HttpRestfulHandler extends ChannelInboundHandlerAdapter {
    private static Logger s_logger = LoggerFactory.getLogger(HttpRestfulHandler.class);

    private static final AsciiString CONTENT_TYPE = new AsciiString("Content-Type");
    private static final AsciiString CONTENT_LENGTH = new AsciiString("Content-Length");
    private static final AsciiString CONNECTION = new AsciiString("Connection");
    private static final AsciiString KEEP_ALIVE = new AsciiString("keep-alive");



    private CommandService commandService;

    public HttpRestfulHandler(CommandService commandService){
        this.commandService = commandService;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if (!(msg instanceof FullHttpRequest)) {
            return;
        }


        FullHttpRequest req = (FullHttpRequest) msg;// 客户端的请求对象
        HttpHeaders headers = req.headers();
        Gson gson = new Gson();
        RespContent resp = new RespContent();
        if (req.method() != HttpMethod.POST || !"application/json".equals(headers.get(CONTENT_TYPE))){

            resp.setCode(CommandServerRespCode.OTHER_ERROR);
            resp.setDescInfo("not http post application/json request!! ");
            responseJson(ctx, req, gson.toJson(resp));
            s_logger.error("request from" + req.uri()+" is not a http post application/json request!!" );
            return;
        }


        String uri = req.uri();// 获取客户端的URL
        s_logger.debug("client "+uri.toString());

        // 根据不同的请求API做不同的处理(路由分发)
        if (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length()-1);
        }

        if ("/rest".equals(uri)) {
            String  reqContent = getRequestContent(req);
            if(StringUtil.isBlank(reqContent)){//
                resp.setCode(CommandServerRespCode.OTHER_ERROR);
                resp.setDescInfo("no json data ");
                responseJson(ctx, req, gson.toJson(resp));
                return ;
            }


            ReqContent reqObj = gson.fromJson(reqContent,ReqContent.class);
            resp = commandService.executeCommand(reqObj);
            responseJson(ctx, req, gson.toJson(resp));
        }

    }

    /**
     * 响应HTTP的请求
     *
     * @param ctx
     * @param req
     * @param jsonStr
     */
    private void responseJson(ChannelHandlerContext ctx, FullHttpRequest req, String jsonStr) {

        boolean keepAlive = HttpUtil.isKeepAlive(req);
        byte[] jsonByteByte = jsonStr.getBytes();
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(jsonByteByte));
        response.headers().set(CONTENT_TYPE, "text/json");
        response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());

        if (!keepAlive) {
            ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        } else {
            response.headers().set(CONNECTION, KEEP_ALIVE);
            ctx.write(response);
        }
    }



    /**
     * 获取请求的内容
     *
     * @param request
     * @return
     */
    private String getRequestContent(FullHttpRequest request) {
        ByteBuf jsonBuf = request.content();
        return jsonBuf.toString(CharsetUtil.UTF_8);
    }
}
