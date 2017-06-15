package org.ling.kafka;

import java.util.Properties;
import java.io.File;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.configuration.PropertiesConfiguration;

public class kafkaConnectAsConsumer {

    public static void main(String args[]) throws Exception {

        Options options = new Options();

        Option output = new Option("o", "output", true, "The output directory of results");
        output.setRequired(true);
        options.addOption(output);

        Option configFile = new Option("f", "file", true, "The configuration file of kafka");
        configFile.setRequired(true);
        options.addOption(configFile);

        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption("f") || !commandLine.hasOption("o")) {
            return;
        }

        File configurationFile = new File(commandLine.getOptionValue('f'));

        PropertiesConfiguration configuration = new PropertiesConfiguration(configurationFile);

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConfigConstants.BOOTSTRAP_SERVERS,
                configuration.getString(ConfigConstants.BOOTSTRAP_SERVERS, ConfigConstants.DEFAULT_BOOTSRAP_SERVERS));
        // only required for Kafka 0.8
        properties.setProperty(ConfigConstants.ZOOKEEPER_CONNECT,
                configuration.getString(ConfigConstants.ZOOKEEPER_CONNECT, ConfigConstants.DEFAULT_ZOOKEEPER_CONNECT));
        properties.setProperty(ConfigConstants.GROUP_ID,
                configuration.getString(ConfigConstants.GROUP_ID, ConfigConstants.DEFAULT_GROUP_ID));
        properties.setProperty(ConfigConstants.CLIENT_ID,
                configuration.getString(ConfigConstants.CLIENT_ID, ConfigConstants.DEFAULT_CLIENT_ID));

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(configuration.getString(ConfigConstants.TOPIC_NAME,
                ConfigConstants.DEFAULT_TOPIC_NAME), new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        stream.writeAsText(commandLine.getOptionValue("o"));

        env.execute();
    }
}
