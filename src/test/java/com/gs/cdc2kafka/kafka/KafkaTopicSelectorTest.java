package com.gs.cdc2kafka.kafka;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class KafkaTopicSelectorTest {

  @Test
  public void isNumericTest() {
    String s1 = "20230101";
    Assert.assertEquals(StringUtils.isNumeric(s1), true);
  }

  @Test
  public void toHumpTest() {
    KafkaTopicSelector kafkaTopicSelector = new KafkaTopicSelector();
    String s1 = "incident_event_history_20230115";
    String s2 = "incident_event_20230115_history";
    Assert.assertEquals(kafkaTopicSelector.toHump(s1), "incidentEventHistory");
    Assert.assertEquals(kafkaTopicSelector.toHump(s2), "incidentEvent20230115History");
  }
}
