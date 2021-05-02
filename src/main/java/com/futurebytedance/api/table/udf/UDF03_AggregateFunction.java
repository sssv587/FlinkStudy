package com.futurebytedance.api.table.udf;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/5/2 - 14:53
 * @Description FlinkAPI - 自定义 AggregateFunction
 */
public class UDF03_AggregateFunction {
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
        AvgTemp avgTemp = new AvgTemp();

        // 需要在环境中注册UDF
        tableEnv.registerFunction("avgTemp", avgTemp);
        Table resultTable = sensorTable
                .groupBy("id")
                .aggregate("avgTemp(temp) as avgtemp")
                .select("id,avgtemp");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,avgTemp(temp) as avgtemp"
                + " from sensor group by id");

        tableEnv.toRetractStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("resultSqlTable");

        env.execute();
    }

    // 实现自定义的AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个accumulate方法，来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1++;
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }
}
