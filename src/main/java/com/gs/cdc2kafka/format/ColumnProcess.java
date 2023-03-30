package com.gs.cdc2kafka.format;

import com.gs.cdc2kafka.bean.GsKafka;
import com.gs.cdc2kafka.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import sun.misc.BASE64Encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class ColumnProcess extends RichMapFunction<GsKafka, String> {

  private final static HashMap<String, HashMap<String, List<String>>> gzipColumn = new HashMap<>();

  private final static BASE64Encoder encoder = new BASE64Encoder();

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    addGzipColumn("gs_task.task_content.content");
  }

  private void addGzipColumn(String column) {
    if (StringUtils.isBlank(column)) {
      return;
    }
    String[] s = column.split("\\.");
    if (s.length != 3) {
      throw new RuntimeException("gzip column configuration is not correct");
    }
    Map<String, List<String>> columnMap = gzipColumn.computeIfAbsent(s[0], (x)-> new HashMap<>());
    columnMap.compute(s[1], (key, value)-> {
      if (value == null) {
        value = new ArrayList<>();
      }
      value.add(s[2]);
      return value;
    });
  }

  @Override
  public String map(GsKafka gsKafka) {
    if (!gzipColumn.containsKey(gsKafka.getDbName())
        || !gzipColumn.get(gsKafka.getDbName()).containsKey(gsKafka.getTableName())) {
      return gsKafka.getJsonNode().toString();
    }

    gzipColumn.get(gsKafka.getDbName()).get(gsKafka.getTableName()).forEach(col-> {
      ObjectNode objectNode = (ObjectNode)gsKafka.getJsonNode();
      if (objectNode.has(col)) {
        String value = objectNode.get(col).asText();
        String gzip = encoder.encode(compress(value));
        objectNode.set(col, JsonUtil.toTextNode(gzip));
      }
    });
    return gsKafka.getJsonNode().toString();
  }

  public static byte[] compress(String str) {
    if (str == null || str.length() == 0) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip;
    try {
      gzip = new GZIPOutputStream(out);
      gzip.write(str.getBytes(StandardCharsets.UTF_8));
      gzip.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }
}
