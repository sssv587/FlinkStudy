package com.futurebytedance.api.sink;

import com.futurebytedance.api.beans.SensorReading;
import com.futurebytedance.api.source.Source04_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/19 - 22:17
 * @Description Flink-Sink-MySQL
 */
public class Sink04_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        //从文件读取数据
//        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\sensor.txt");
//
//        //转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
//            String[] fields = value.split(",");
//            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
//        });
        DataStreamSource<SensorReading> dataStream = env.addSource(new Source04_UDF.MySensorSource());

        dataStream.addSink(new MyJDBCSink());


        env.execute();
    }

    //实现自定义SinkFunction
    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        //声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            insertStmt = connection.prepareStatement("insert into sensor_temp(id,temp) values (?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        // 每来一条语句，调用连接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws SQLException {
            //直接执行更新语句，如果没有更新，那么就插入
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
