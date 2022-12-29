package com.gs.cdc2kafka.format;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.MapFunction;

public class FormatFunction implements MapFunction<String, String> {

  private final static Gson gson = new Gson();

  @Override
  public String map(String value) throws Exception {
    JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
    JsonObject after = jsonObject.getAsJsonObject("after");
    JsonObject source = jsonObject.getAsJsonObject("source");
    String dbName = source.get("db").getAsString();
    String tableName = source.get("table").getAsString();
    if (after == null) { //暂时不处理删除操作
      return null;
    }
    after.addProperty("cdc_db_name", dbName);
    after.addProperty("cdc_table_name", tableName);
    return after.toString();
  }
}
