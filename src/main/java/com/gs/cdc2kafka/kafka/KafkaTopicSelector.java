package com.gs.cdc2kafka.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.kafka.sink.TopicSelector;

public class KafkaTopicSelector implements TopicSelector<String> {

  private final static Gson gson = new Gson();

  @Override
  public String apply(String s) {
    JsonObject jsonObject = gson.fromJson(s, JsonObject.class);
    JsonObject meta = jsonObject.getAsJsonObject("cdc_meta");
    String dbName = meta.get("db_name").getAsString();
    String tableName = meta.get("table_name").getAsString();
    return String.format("bigdata.ods.%s.%s", toHump(dbName), toHump(tableName));
  }

  @VisibleForTesting
  protected String toHump(String s) {
    String[] names = s.split("_");
    StringBuilder sb = new StringBuilder();
    sb.append(names[0]);
    for (int i=1; i<names.length; i++) {
      if (i == names.length -1 && StringUtils.isNumeric(names[i])) {
        continue;
      }
      sb.append(names[i].substring(0, 1).toUpperCase()).append(names[i].substring(1));
    }
    return sb.toString();
  }
}
