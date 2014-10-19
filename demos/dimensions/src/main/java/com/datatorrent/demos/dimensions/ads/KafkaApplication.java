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
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.demos.dimensions.ads.AdInfo.AdInfoAggregator;
import com.datatorrent.lib.statistics.DimensionsComputation;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates AdsDimensions demo writing dimensions calculations to Kafka.  Message format
 * is determined by KafkaStringEncoder and is of the following format
 *      publisherId, advertiserId, adUnit, timestamp, cost, revenue, impressions, clicks
 *
 * Default Kafka topic is set to adsdimensions and can be changed with
 *
 *    <property>
 *      <name>dt.operator.Kafka.prop.topic</name>
 *      <value>adsdimensions</value>
 *    </property>
 *
 * Kafka Brokers List can be set with following block in dt-site.xml or application configuration file
 *
 *    <property>
 *      <name>dt.operator.Kafka.prop.configProperties(metadata.broker.list)</name>
 *      <value>localhost:9092</value>
 *    </property>
 *
 * Additional Kafka properties can be found in http://kafka.apache.org/documentation.html#producerconfigs
 * and can be set using following pattern
 *
 *    <property>
 *      <name>dt.operator.Kafka.prop.configProperties(producer.type)</name>
 *      <value>async</value>
 *    </property>
 *
 * @since 1.0.3
 */
@ApplicationAnnotation(name="KafkaAdsDimensionsDemo")
public class KafkaApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);

    DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<AdInfo, AdInfo.AdInfoAggregateEvent>());
    dag.setAttribute(dimensions, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 60);

    String[] dimensionSpecs = new String[] {
            "time=" + TimeUnit.MINUTES,
            "time=" + TimeUnit.MINUTES + ":adUnit",
            "time=" + TimeUnit.MINUTES + ":advertiserId",
            "time=" + TimeUnit.MINUTES + ":publisherId",
            "time=" + TimeUnit.MINUTES + ":advertiserId:adUnit",
            "time=" + TimeUnit.MINUTES + ":publisherId:adUnit",
            "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId",
            "time=" + TimeUnit.MINUTES + ":publisherId:advertiserId:adUnit"
    };

    AdInfoAggregator[] aggregators = new AdInfoAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      AdInfoAggregator aggregator = new AdInfoAggregator();
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }

    dimensions.setAggregators(aggregators);
    DimensionsComputationUnifierImpl unifier = new DimensionsComputationUnifierImpl();
    unifier.setAggregators(aggregators);
    dimensions.setUnifier(unifier);

    KafkaSinglePortOutputOperator kafka = dag.addOperator("Kafka", new KafkaSinglePortOutputOperator<String, AdInfo>());

    // Set default Kafka topic for writing output messages
    kafka.setTopic("adsdimensions");
    // Set default properties based on http://kafka.apache.org/documentation.html#producerconfigs
    Properties kafkaProps = kafka.getConfigProperties();
    kafkaProps.setProperty("serializer.class", KafkaStringEncoder.class.getName());

    dag.addStream("InputStream", input.outputPort, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, kafka.inputPort);
  }


  public static class KafkaStringEncoder implements kafka.serializer.Encoder<AdInfo>
  {

    public KafkaStringEncoder(kafka.utils.VerifiableProperties props) {
    }

    /**
     * Encodes a single AdInfo object as a string in CSV format with following fields:
     *      publisherId, advertiserId, adUnit, timestamp, cost, revenue, impressions, clicks
     * @param adInfo
     * @return
     */
    @Override
    public byte[] toBytes(AdInfo adInfo) {

      StringBuilder msg = new StringBuilder(100);
      msg.append(adInfo.publisherId).append(",");
      msg.append(adInfo.advertiserId).append(",");
      msg.append(adInfo.adUnit).append(",");
      msg.append(adInfo.timestamp).append(",");
      msg.append(adInfo.cost).append(",");
      msg.append(adInfo.revenue).append(",");
      msg.append(adInfo.impressions).append(",");
      msg.append(adInfo.clicks);

      return msg.toString().getBytes();
    }

  }


}

