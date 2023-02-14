package com.gs.cdc2kafka.format;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.ConnectSchema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.errors.DataException;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.utils.DateTimeUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;

public class JsonConverter implements Serializable {

  private final ZoneId zoneId;

  private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

  public JsonConverter(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  public ObjectNode convert2ObjectNode(Schema schema, Struct struct) {
    if (!struct.schema().equals(schema))
      throw new DataException("Mismatching schema.");
    ObjectNode obj = JSON_NODE_FACTORY.objectNode();
    for (Field field : schema.fields()) {
      obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
    }
    return obj;
  }

  public JsonNode convertToJson(Schema schema, Object value) {
    if (value == null) {
      if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
        return null;
      if (schema.defaultValue() != null)
        return convertToJson(schema, schema.defaultValue());
      if (schema.isOptional())
        return JSON_NODE_FACTORY.nullNode();
      throw new DataException("Conversion error: null value for field that is required and has no default value");
    }

    try {
      final Schema.Type schemaType;
      if (schema == null) {
        schemaType = ConnectSchema.schemaType(value.getClass());
        if (schemaType == null)
          throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
      } else {
        schemaType = schema.type();
      }
      return isTimeSchema(schema.name()) ? convertToTime(schema.name(), value) : convertByType(schemaType, schema, value);
    } catch (ClassCastException e) {
      String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
      throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
    }
  }

  private JsonNode convertByType(Schema.Type schemaType, Schema schema, Object value) {

    switch (schemaType) {
      case INT8:
        return JSON_NODE_FACTORY.numberNode((Byte) value);
      case INT16:
        return JSON_NODE_FACTORY.numberNode((Short) value);
      case INT32:
        return JSON_NODE_FACTORY.numberNode((Integer) value);
      case INT64:
        return JSON_NODE_FACTORY.numberNode((Long) value);
      case FLOAT32:
        return JSON_NODE_FACTORY.numberNode((Float) value);
      case FLOAT64:
        return JSON_NODE_FACTORY.numberNode((Double) value);
      case BOOLEAN:
        return JSON_NODE_FACTORY.booleanNode((Boolean) value);
      case STRING:
        CharSequence charSeq = (CharSequence) value;
        return JSON_NODE_FACTORY.textNode(charSeq.toString());
      case BYTES:
        if (value instanceof byte[])
          return JSON_NODE_FACTORY.binaryNode((byte[]) value);
        else if (value instanceof ByteBuffer)
          return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
        else
          throw new DataException("Invalid type for bytes type: " + value.getClass());
      case ARRAY: {
        Collection collection = (Collection) value;
        ArrayNode list = JSON_NODE_FACTORY.arrayNode();
        for (Object elem : collection) {
          Schema valueSchema = schema == null ? null : schema.valueSchema();
          JsonNode fieldValue = convertToJson(valueSchema, elem);
          list.add(fieldValue);
        }
        return list;
      }
      case MAP: {
        Map<?, ?> map = (Map<?, ?>) value;
        // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
        boolean objectMode;
        if (schema == null) {
          objectMode = true;
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String)) {
              objectMode = false;
              break;
            }
          }
        } else {
          objectMode = schema.keySchema().type() == Schema.Type.STRING;
        }
        ObjectNode obj = null;
        ArrayNode list = null;
        if (objectMode)
          obj = JSON_NODE_FACTORY.objectNode();
        else
          list = JSON_NODE_FACTORY.arrayNode();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          Schema keySchema = schema == null ? null : schema.keySchema();
          Schema valueSchema = schema == null ? null : schema.valueSchema();
          JsonNode mapKey = convertToJson(keySchema, entry.getKey());
          JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

          if (objectMode)
            obj.set(mapKey.asText(), mapValue);
          else
            list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
        }
        return objectMode ? obj : list;
      }
      case STRUCT: {
        Struct struct = (Struct) value;
        if (!struct.schema().equals(schema))
          throw new DataException("Mismatching schema.");
        ObjectNode obj = JSON_NODE_FACTORY.objectNode();
        for (Field field : schema.fields()) {
          obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
        }
        return obj;
      }
    }
    throw new DataException("Couldn't convert " + value + " to JSON.");
  }

  private JsonNode convertToTime(String name, Object value) {
    switch (name) {
      case Timestamp.SCHEMA_NAME:
        LocalDateTime localDateTime01 = TimestampData.fromEpochMillis((Long) value).toLocalDateTime();
        return JSON_NODE_FACTORY.textNode(localDateTime01.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
      case MicroTimestamp.SCHEMA_NAME:
        long micro = (long) value;
        LocalDateTime localDateTime02 = TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000)).toLocalDateTime();
        return JSON_NODE_FACTORY.textNode(localDateTime02.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
      case NanoTimestamp.SCHEMA_NAME:
        long nano = (long) value;
        LocalDateTime localDateTime03 = TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000)).toLocalDateTime();
        return JSON_NODE_FACTORY.textNode(localDateTime03.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
      case ZonedTimestamp.SCHEMA_NAME:
        LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(value, zoneId);
        return JSON_NODE_FACTORY.textNode(localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
      case MicroTime.SCHEMA_NAME:
        return JSON_NODE_FACTORY.textNode(convertMicroTime(value).format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      case NanoTime.SCHEMA_NAME:
        return JSON_NODE_FACTORY.numberNode((int) ((long) value / 1000_000));
      case Time.SCHEMA_NAME:
      case ZonedTime.SCHEMA_NAME:
        return JSON_NODE_FACTORY.textNode(convertTime(value).format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      case Date.SCHEMA_NAME:
        return JSON_NODE_FACTORY.textNode(convertDate(value).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
      default:
        throw new RuntimeException(String.format("no supported type as %s", name));
    }
  }

  private boolean isTimeSchema(String str) {
    if (StringUtils.isBlank(str))  {
      return false;
    }
    return Timestamp.SCHEMA_NAME.equals(str)
        || MicroTimestamp.SCHEMA_NAME.equals(str)
        || NanoTimestamp.SCHEMA_NAME.equals(str)
        || ZonedTimestamp.SCHEMA_NAME.equals(str)
        || MicroTime.SCHEMA_NAME.equals(str)
        || NanoTime.SCHEMA_NAME.equals(str)
        || Time.SCHEMA_NAME.equals(str)
        || ZonedTime.SCHEMA_NAME.equals(str)
        || Date.SCHEMA_NAME.equals(str);
  }


  private JsonNode toTextNode(TimestampData timestampData) {
    return JSON_NODE_FACTORY.textNode(timestampData.toString());
  }

  private LocalDate convertDate(Object value) {
    int dateInt = (int) TemporalConversions.toLocalDate(value).toEpochDay();
    return DateTimeUtils.toLocalDate(dateInt);
  }

  private LocalTime convertTime(Object value) {
    int timeInt = TemporalConversions.toLocalTime(value).toSecondOfDay() * 1000;
    return DateTimeUtils.toLocalTime(timeInt);
  }

  private LocalTime convertMicroTime(Object value) {
    int timeInt = (int) ((long) value / 1000);
    return DateTimeUtils.toLocalTime(timeInt);
  }

  public void addMeta(Struct source, ObjectNode objectNode, Envelope.Operation op) {
    String db = source.getString("db");
    String table = source.getString("table");
    ObjectNode cdcMeta = JSON_NODE_FACTORY.objectNode();
    cdcMeta.set("db_name", JSON_NODE_FACTORY.textNode(db));
    cdcMeta.set("table_name", JSON_NODE_FACTORY.textNode(table));
    cdcMeta.set("operation", JSON_NODE_FACTORY.textNode(op.name()));
    objectNode.set("cdc_meta", cdcMeta);
  }
}
