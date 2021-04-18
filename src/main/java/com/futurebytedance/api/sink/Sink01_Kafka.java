package com.futurebytedance.api.sink;

import com.futurebytedance.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/4/18 - 23:21
 * @Description Flink-Sink-Kafka
 */
public class Sink01_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> inputStream = env.addSource(new FlinkKafkaConsumer011<>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = inputStream.map((MapFunction<String, String>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "sinktest", new SimpleStringSchema()));

        env.execute();
    }
}
