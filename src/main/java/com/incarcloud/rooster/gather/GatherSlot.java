package com.incarcloud.rooster.gather;

import com.incarcloud.rooster.datapack.DataPack;
import com.incarcloud.rooster.datapack.IDataParser;
import com.incarcloud.rooster.mq.MQException;
import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InvalidClassException;
import java.util.List;


// 采集处理槽 一个处理槽有一个采集端口，一个包解析器，结果输出给MQ
public abstract class GatherSlot {
    private static Logger s_logger = LoggerFactory.getLogger(GatherSlot.class);

    private GatherHost _host;
    private IDataParser _dataParser;

    GatherSlot(GatherHost host) {
        _host = host;
    }

    public void setDataParser(String parser) throws InvalidClassException,
            ClassNotFoundException, IllegalAccessException, InstantiationException {
        setDataParser(parser, "com.incarcloud.rooster.datapack");
    }

    IDataParser getDataParser(){ return _dataParser; }

    public void setDataParser(String parser, String pack) throws InvalidClassException,
            ClassNotFoundException, IllegalAccessException, InstantiationException {
        // 利用反射构造出对应的解析器对象
        String fullName = String.format("%s.%s", pack, parser);
        Class<?> ParserClass = Class.forName(fullName);
        IDataParser dataParser = (IDataParser) ParserClass.newInstance();
        if (dataParser == null)
            throw new InvalidClassException(String.format("%s does not implement interface %s",
                    fullName, IDataParser.class.toString()));

        _dataParser = dataParser;
    }

    abstract void start();

    abstract void stop();

    // 投递到MQ
    Promise<Object,List<MQException>, Object> postMQ(List<DataPack> listPacks){
        return _host.postMQ(listPacks);
    }
}