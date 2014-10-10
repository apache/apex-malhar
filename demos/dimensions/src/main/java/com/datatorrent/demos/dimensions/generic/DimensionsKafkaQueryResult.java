/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.demos.dimensions.ads.KafkaJsonEncoder;

/**
 * Sends app data query results as JSON strings to Kafka
 * <p>
 *
 * @displayName Dimensions Kafka Query Result
 * @category Messaging
 * @tags appdata, query, kafka
 */
public class DimensionsKafkaQueryResult extends KafkaSinglePortOutputOperator
{
  public static final String DEFAULT_SERIALIZER = KafkaJsonEncoder.class.getCanonicalName();
  {
    setTopic("GenericDimensionsQueryResult");
    getConfigProperties().setProperty("serializer.class", DEFAULT_SERIALIZER);
  }

  public void setBrokerSet(String brokerSet) {
    getConfigProperties().setProperty("metadata.broker.list", brokerSet);
  }
  public String getBrokerSet() {
    return getConfigProperties().getProperty("metadata.broker.list");
  }

  public void setSerializer(String serializer) {
    getConfigProperties().setProperty("serializer.class", serializer);
  }
  public String getSerializer() {
    return getConfigProperties().getProperty("serializer.class");
  }


}
