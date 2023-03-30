package com.gs.cdc2kafka.bean;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class GsKafka {

  private String dbName;

  private String tableName;

  private JsonNode jsonNode;

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public JsonNode getJsonNode() {
    return jsonNode;
  }

  public void setJsonNode(JsonNode jsonNode) {
    this.jsonNode = jsonNode;
  }
}
