package com.gs.cdc2kafka;

import com.gs.cdc2kafka.format.GsDebeziumDeserializeSchema;
import com.gs.cdc2kafka.kafka.KafkaTopicSelector;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

public class StreamJob {

  private final static String HOST = "host";
  private final static String PORT = "port";
  private final static String DB_NAME = "dbName";
  private final static String TABLES = "tables";
  private final static String USER_NAME = "userName";
  private final static String PASSWORD = "password";
  private final static String BOOTSTRAP_SERVERS = "bootstrap.servers";
  private final static String STARTUP_MODE = "startupMode";
  private final static String TIMESTAMP = "timestamp";
  private final static String TRANSACTIONAL_ID = "transactional.id";
  private final static String JOB_NAME = "job.name";
  private final static String SERVER_ID = "server.id";
  private final static String SERVER_TIME_ZONE = "Asia/Shanghai";

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    checkParam(params);
    MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
        .hostname(params.get(HOST))
        .port(params.getInt(PORT, 3306))
        .scanNewlyAddedTableEnabled(true)
        .serverId(params.get(SERVER_ID))
        .databaseList(params.get(DB_NAME)) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
        .tableList(params.get(TABLES)) // 设置捕获的表
        .username(params.get(USER_NAME))
        .password(params.get(PASSWORD))
        .serverTimeZone(SERVER_TIME_ZONE)
        .deserializer(new GsDebeziumDeserializeSchema(SERVER_TIME_ZONE)); // 将 SourceRecord 转换为 JSON 字符串

    MySqlSource<String> mySqlSource = buildStartup(builder, params).build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    init(env);

    Properties properties = new Properties();
    //transaction超时时间
    properties.put("transaction.timeout.ms", 15*60*1000);
    env
        .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
        .sinkTo(KafkaSink.<String>builder()
            .setBootstrapServers(params.get(BOOTSTRAP_SERVERS))
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopicSelector(new KafkaTopicSelector())
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix(params.get(TRANSACTIONAL_ID))
            .setKafkaProducerConfig(properties)
            .build());
    env.execute(params.get(JOB_NAME));
  }

  private static void init(StreamExecutionEnvironment env) {
    env.setParallelism(1);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointInterval(10*1000);
    env.getCheckpointConfig().setCheckpointTimeout(10*60*1000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5*1000);
  }

  private static MySqlSourceBuilder<String> buildStartup(MySqlSourceBuilder<String> builder, ParameterTool params) {
    String startupMode = params.get(STARTUP_MODE, "latest");
    switch (startupMode) {
      case "initial":
        builder.startupOptions(StartupOptions.initial());
        break;
      case "latest":
        builder.startupOptions(StartupOptions.latest());
        break;
      case "earliest":
        builder.startupOptions(StartupOptions.earliest());
        break;
      case "timestamp":
        builder.startupOptions(StartupOptions.timestamp(params.getLong(TIMESTAMP)));
        break;
      default:
        throw new RuntimeException("no supported startupMode");
    }
    return builder;
  }

  private static void checkParam(ParameterTool params) {
    Preconditions.checkNotNull(params.get(JOB_NAME), "job name can not be null");
    Preconditions.checkNotNull(params.get(SERVER_ID), "server id can not be null");
    Preconditions.checkNotNull(params.get(HOST), "db host can not be null");
    Preconditions.checkNotNull(params.get(DB_NAME), "db name can not be null");
    Preconditions.checkNotNull(params.get(TABLES), "tables can not be null");
    Preconditions.checkNotNull(params.get(USER_NAME), "user name can not be null");
    Preconditions.checkNotNull(params.get(PASSWORD), "password can not be null");
    Preconditions.checkNotNull(params.get(TRANSACTIONAL_ID), "transactional.id can not be null");
    if (TIMESTAMP.equals(params.get(STARTUP_MODE))) {
      Preconditions.checkNotNull(params.get(TIMESTAMP), "when startup mode is timestamp, timestamp can not be null");
    }
    Preconditions.checkNotNull(params.get(BOOTSTRAP_SERVERS), "kafka bootstrap.servers can not be null");
  }
}
