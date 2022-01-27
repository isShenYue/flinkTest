package com;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @Name Sink_JDBC
 * @Author shenYue
 * @Data 2022/1/26 16:03
 **/
public class Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局线程数
        env.setParallelism(1);
        //读取规则表 目前是jdbc进行连接读取
        DataStreamSource<List<HashMap<String,Object>>> dataStreamSource = env.addSource(new MysqlSoucer());
        //打印规则表内容
        dataStreamSource.print();
        //会把规则表的信息传给MyjdbcSink的invoke方法
        DataStreamSink<List<HashMap<String,Object>>> integerDataStreamSink = dataStreamSource.addSink(new MyJdbcSink());
        //开始执行
        env.execute();
    }

    public static class MysqlSoucer extends RichParallelSourceFunction<List<HashMap<String,Object>>>{
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection= DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useSSL=false&serverTimezone=GMT","root","root123");
            String sql="select * from big_data_rule";
            preparedStatement= connection.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext sourceContext) throws Exception {
            ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
            HashMap<String, Object> map = new HashMap<>();
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                int anInt = resultSet.getInt(1);
                String tabeleName = resultSet.getString(2);
                String rule = resultSet.getString(3);
                String tableColumn = resultSet.getString(4);
                map.put("id",anInt);
                map.put("tableName",tabeleName);
                map.put("rule",rule);
                map.put("tableColumn",tableColumn);
                list.add(map);
            }
            sourceContext.collect(list);
        }

        @Override
        public void cancel() {

        }


        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }

    public static class MyJdbcSink extends RichSinkFunction<List<HashMap<String,Object>>>{
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection= DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test?characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useSSL=false&serverTimezone=GMT","root","root123");
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void invoke(List<HashMap<String,Object>> list, Context context) throws Exception {
            for (int i = 0;i<list.size();i++){
                HashMap<String, Object> map = list.get(0);
                String sql="select id,"+map.get("tableColumn")+" from "+map.get("tableName");
                preparedStatement= connection.prepareStatement(sql);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    int id = resultSet.getInt(1);
                    String string = resultSet.getString(2);
                    /*System.out.println(id);
                    System.out.println(string);*/
                    String newString = string.replaceAll(" ", "");
                    System.out.println(newString);
                    PreparedStatement preparedStatement = connection.prepareStatement("update "+map.get("tableName")+" set " + map.get("tableColumn") + "='" + newString + "' where" + " id=" + id);
                    int i1 = preparedStatement.executeUpdate();
                    System.out.println(i1);
                }
            }
        }
    }
}
