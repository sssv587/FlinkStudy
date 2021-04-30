package com.futurebytedance.api.table;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/30 - 22:43
 * @Description FlinkAPI-FlinkSQL
 */
public class Table01_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.读取数据
        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\sensor.txt");

        // 2.转换成POJO
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) {
                String[] field = value.split(",");
                return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
            }
        });

        // 3.创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv =  StreamTableEnvironment.create(env, settings);

        // 4.基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5.调用table API进行转换操作
        Table resultTable = dataTable.select("id,temperature")
                .where("id = sensor_1");

        // 6.执行SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id,temperature from sensor where id = 'sensor_1'";

        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
