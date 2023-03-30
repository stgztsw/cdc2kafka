package com.gs.cdc2kafka.format;

import com.gs.cdc2kafka.bean.GsKafka;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import java.time.ZoneId;
import java.util.Optional;

public class GsDebeziumDeserializeSchema implements DebeziumDeserializationSchema<GsKafka> {

  private JsonConverter jsonConverter;

  public GsDebeziumDeserializeSchema(String serverTimeZone) {
    ZoneId zoneId = Optional.of(serverTimeZone).map(ZoneId::of).orElseGet(ZoneId::systemDefault);
    this.jsonConverter = new JsonConverter(zoneId);
  }

  @Override
  public void deserialize(SourceRecord record, Collector<GsKafka> out) throws Exception {
    Envelope.Operation op = Envelope.operationFor(record);
    Struct value = (Struct) record.value();
    Schema valueSchema = record.valueSchema();
    //delete场景
    if (value.getStruct(Envelope.FieldName.AFTER) == null) {
      return;
    }
    GsKafka upsert = extractAfterRow(value, valueSchema, op);
    out.collect(upsert);
  }

  private GsKafka extractAfterRow(Struct value, Schema valueSchema, Envelope.Operation op) throws Exception {
    Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
    Struct after = value.getStruct(Envelope.FieldName.AFTER);
    ObjectNode objectNode = jsonConverter.convert2ObjectNode(afterSchema, after);
    GsKafka gsKafka = jsonConverter.addMeta(source, objectNode, op);
    return gsKafka;
  }

  @Override
  public TypeInformation<GsKafka> getProducedType() {
    return TypeInformation.of(GsKafka.class);
  }

}
