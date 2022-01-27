package com.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Name SourceTestKafka
 * @Author shenYue
 * @Data 2022/1/17 11:46
 **/
public class SourceTestKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("flinkTest", new SimpleStringSchema(), properties));

        source.print();
        env.execute();
    }
}
