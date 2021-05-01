package com.futurebytedance.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/5/1 - 13:54
 * @Description FlinkSQL- Table-API - Kafka数据管道
 */
public class Table04_KafkaPipeline {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.连接Kafka
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 3.查询转换
        // 简单转换
        Table sensorTable = tableEnv.from("inputTable");

        Table resultTable = sensorTable.select("id,temperature")
                .filter("id = 'sensor_6'");

        // 聚合操作
//        Table aggTable = sensorTable.groupBy("id")
//                .select("id,id.count as count,temperature.avg as avgTemp");

        // 4.建立kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sink_test")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
