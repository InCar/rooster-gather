package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.IDataParser;

import java.io.InvalidClassException;
import java.util.Date;

/**
 * <p>
 * 采集处理槽父类
 * </p>
 * <p>
 * 一个处理槽有一个采集端口，一个包解析器，结果输出给MQ
 *
 * @author 熊广化
 */
public abstract class GatherSlot {

    /**
     * 名称
     */
    protected String name;

    /**
     * 采集槽所在主机
     */
    protected GatherHost _host;
    /**
     * 数据包解析器
     */
    protected IDataParser _dataParser;

    /**
     * @param host 采集槽所在主机
     */
    GatherSlot(GatherHost host) {
        _host = host;
        this.name = _host.getName() + "-" + "slot" + new Date().getTime();
    }

    /**
     * @param name  采集槽名称
     * @param _host 采集槽所在主机
     */
    GatherSlot(String name, GatherHost _host) {
        this.name = name;
        this._host = _host;
    }

    public void setDataParser(String parser)
            throws InvalidClassException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        setDataParser(parser, "com.incarcloud.rooster.datapack");
    }

    /**
     * 设置数据解析器类
     *
     * @param parser 类名
     * @param pack   类所在包名
     * @throws InvalidClassException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    void setDataParser(String parser, String pack)
            throws InvalidClassException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        // 利用反射构造出对应的解析器对象
        String fullName = String.format("%s.%s", pack, parser);
        Class<?> ParserClass = Class.forName(fullName);
        IDataParser dataParser = (IDataParser) ParserClass.newInstance();
        if (dataParser == null)
            throw new InvalidClassException(
                    String.format("%s does not implement interface %s", fullName, IDataParser.class.toString()));

        _dataParser = dataParser;
    }

    public IDataParser getDataParser() {
        return _dataParser;
    }

    /**
     * 开始运行
     */
    public synchronized void start() {
        if (null == _dataParser) {
            throw new RuntimeException("dataParse is  null !!");
        }

        if (null == _host) {
            throw new RuntimeException("host is  null !!");
        }

        start0();
    }

    protected abstract void start0();

    /**
     * 停止
     */
    public abstract void stop();

    /**
     * 获取传输协议
     *
     * @return tcp/udp/mqtt
     */
    public abstract String getTransportProtocal();

    /**
     * 获取监听端口
     *
     * @return
     */
    public abstract int getListenPort();

    /**
     * 将数据包处理任务扔到队列中
     *
     * @param packWrap
     */
    public void putToCacheQueue(DataPackWrap packWrap) {
        _host.putToCacheQueue(packWrap);
    }

    /**
     * 获取 名称
     *
     * @return name
     */
    public String getName() {
        return name;
    }
}