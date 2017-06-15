package org.ling.kafka;

/**
 * Created by link on 2017/6/15.
 */
public final class ConfigConstants {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String DEFAULT_BOOTSRAP_SERVERS = "localhost:9092";

    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";

    public static final String TOPIC_NAME = "topic";
    public static final String DEFAULT_TOPIC_NAME = "logPool";

    public static final String GROUP_ID = "group.id";
    public static final String DEFAULT_GROUP_ID = "logPool-group";

    public static final String CLIENT_ID = "client.id";
    public static final String DEFAULT_CLIENT_ID = "logPool-client";

}
