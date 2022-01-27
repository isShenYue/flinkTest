package com.flink;

import com.entity.Sender;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

/**
 * @Name StreamListManage
 * @Author shenYue
 * @Data 2022/1/14 16:46
 **/
public class StreamListManage {
    public static void main(String[] args) throws Exception {
        //流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sender> senderDataStreamSource = env.fromCollection(Arrays.asList(new Sender("data1", 1, ""), new Sender("data2", 2, ""), new Sender("data3", 3, "")));
        senderDataStreamSource.print();
        env.execute();
    }
}
