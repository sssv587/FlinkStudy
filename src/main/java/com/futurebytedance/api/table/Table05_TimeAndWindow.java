package com.futurebytedance.api.table;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/5/1 - 15:22
 * @Description FlinkSQL- 窗口操作 - GroupWindow&OverWindow
 */
public class Table05_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\sensor.txt");

        // 3.转换成POJO
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] field = value.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
            @Override
            public long extractAscendingTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 4.将流转换成表，定义时间特性
        // Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts,temperature as temp, pt.proctime");
        // Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,timestamp.rowtime,temperature");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature,rt.rowtime");
        tableEnv.createTemporaryView("sensor", dataTable);


        // 5.窗口操作
        // 5.1 Group Window
        // table API
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temperature.avg,tw.end");

        // SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) as avg_temp,tumble_end(rt,interval '10' second)  " +
                "from sensor group by id,tumble(rt,interval '10' second)");


        dataTable.printSchema();
        tableEnv.toAppendStream(dataTable, Row.class).print("dataTable");
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable");

        // 5.2 Over Window
        // table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id,rt,id.count over ow,temperature.avg over ow");

        // SQL
        Table overSqlResult = tableEnv.sqlQuery("select id,rt,count(id) over ow,avg(temperature) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");

        tableEnv.toAppendStream(overResult, Row.class).print("overResult");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("overSqlResult");

        env.execute();
    }
}
