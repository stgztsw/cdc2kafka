package com.gs.cdc2kafka.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.connector.kafka.sink.TopicSelector;

public class KafkaTopicSelector implements TopicSelector<String> {

  private final static Gson gson = new Gson();

  @Override
  public String apply(String s) {
    JsonObject jsonObject = gson.fromJson(s, JsonObject.class);
    String dbName = jsonObject.get("cdc_db_name").getAsString();
    String tableName = jsonObject.get("cdc_table_name").getAsString();
    return String.format("bigdata.ods.%s.%s", toHump(dbName), toHump(tableName));
  }

  private String toHump(String s) {
    String[] names = s.split("_");
    StringBuilder sb = new StringBuilder();
    sb.append(names[0]);
    for (int i=1; i<names.length; i++) {
      sb.append(names[i].substring(0,1).toUpperCase() + names[i].substring(1));
    }
    return sb.toString();
  }
}
