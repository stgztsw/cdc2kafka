package com.gs.cdc2kafka.util;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

public class JsonUtil {

  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

  public static TextNode toTextNode(String value) {
    return JSON_NODE_FACTORY.textNode(value);
  }
}
