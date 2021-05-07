package com.futurebytedance.api.table.udf;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/5/2 - 15:20
 * @Description 表聚合函数(Table-Aggregate-Function)
 */
public class UDF04_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\10926\\IdeaProjects\\FlinkStudy\\doc\\txt\\sensor.txt");

        // 2.转换成POJO
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map((MapFunction<String, SensorReading>) value -> {
            String[] field = value.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });

        // 3.将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp");

        // 4.自定义聚合函数，求当前传感器的平均温度值
        // 4.1 table API
        Top2Temp top2Temp = new Top2Temp();

        // 需要在环境中注册UDF
        tableEnv.registerFunction("top2Temp", top2Temp);
        Table resultTable = sensorTable
                .groupBy("id")
                .flatAggregate("top2Temp(temp) as (temp,rank)")
                .select("id,temp,rank");

        // 4.2 SQL 貌似还不支持
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,top2Temp(temp) "
                + " from sensor group by id");

        tableEnv.toRetractStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable");

        env.execute();
    }

    // 自定义状态类
    public static class Top2TempAcc {
        Double highestTemp = Double.MIN_VALUE;
        Double secondHighestTemp = Double.MIN_VALUE;
    }

    // 自定义一个表聚合函数，实现Top2功能，输出(temp,rank)
    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Top2TempAcc> {

        // 初始化状态
        @Override
        public Top2TempAcc createAccumulator() {
            return new Top2TempAcc();
        }

        // 每来一个数据后，聚合计算的操作
        public void accumulate(Top2TempAcc acc, Double temp) {
            // 将当前温度值，跟状态中的最高温和第二高温比较，如果大的话，就替换
            if (temp > acc.highestTemp) {
                // 如果比最高温还高，就排第一，其他温度依次后移
                acc.secondHighestTemp = acc.highestTemp;
                acc.highestTemp = temp;
            } else if (temp > acc.secondHighestTemp && temp < acc.highestTemp) {
                acc.secondHighestTemp = temp;
            }
        }

        // 实现一个输出数据的方法，写入结果表中
        public void emitValue(Top2TempAcc acc, Collector<Tuple2<Double, Integer>> out) {
            out.collect(new Tuple2<>(acc.highestTemp, 1));
            out.collect(new Tuple2<>(acc.secondHighestTemp, 2));
        }
    }
}
