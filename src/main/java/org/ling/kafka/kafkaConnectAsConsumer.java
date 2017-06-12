package org.ling.kafka;

import java.util.Properties;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class kafkaConnectAsConsumer {

    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010("topic", new SimpleStringSchema(), properties));

        stream.print();
    }
}
