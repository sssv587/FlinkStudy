package com.futurebytedance.api.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/5/1 - 0:57
 * @Description FlinkSQL- Table-API - 输出到文件
 */
public class Table03_FileOutput {
    public static void main(String[] args) throws Exception {
        // 1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String path = "C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3.查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id,temperature")
                .filter("id = 'sensor_6'");

        // 聚合操作
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count,temperature.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id,temperature from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) as avgTemp from inputTable group by id");

        // 4.输出到文件
        // 连接外部文件，注册输出表
        String outputPath = "C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                )
                .createTemporaryTable("outputTable");

//        resultTable.insertInto("outputTable");
        aggTable.insertInto("outputTable");

        env.execute();
    }
}
