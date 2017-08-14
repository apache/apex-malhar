/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.frauddetect;

import java.io.Serializable;
import java.net.URI;

import org.apache.apex.examples.frauddetect.operator.HdfsStringOutputOperator;
import org.apache.apex.examples.frauddetect.operator.MongoDBOutputOperator;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.io.PubSubWebSocketInputOperator;
import org.apache.apex.malhar.lib.io.PubSubWebSocketOutputOperator;
import org.apache.apex.malhar.lib.math.RangeKeyVal;
import org.apache.apex.malhar.lib.multiwindow.SimpleMovingAverage;
import org.apache.apex.malhar.lib.util.BaseKeyValueOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.utils.PubSubHelper;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.netlet.util.DTThrowable;


/**
 * Fraud detection application
 *
 * @since 0.9.0
 */
@ApplicationAnnotation(name = "FraudDetectExample")
public class Application implements StreamingApplication
{

  public PubSubWebSocketInputOperator getPubSubWebSocketInputOperator(String name, DAG dag, URI duri, String topic) throws Exception
  {
    PubSubWebSocketInputOperator reqin = dag.addOperator(name, new PubSubWebSocketInputOperator());
    reqin.setUri(duri);
    reqin.setTopic(topic);
    return reqin;
  }

  public PubSubWebSocketOutputOperator getPubSubWebSocketOutputOperator(String name, DAG dag, URI duri, String topic) throws Exception
  {
    PubSubWebSocketOutputOperator out = dag.addOperator(name, new PubSubWebSocketOutputOperator());
    out.setUri(duri);
    return out;
  }

  public HdfsStringOutputOperator getHdfsOutputOperator(String name, DAG dag, String folderName)
  {
    HdfsStringOutputOperator oper = dag.addOperator("hdfs", HdfsStringOutputOperator.class);
    oper.setFilePath(folderName);
    oper.setMaxLength(1024 * 1024 * 1024);
    return oper;
  }

  public ConsoleOutputOperator getConsoleOperator(String name, DAG dag, String prefix, String format)
  {
    ConsoleOutputOperator oper = dag.addOperator(name, ConsoleOutputOperator.class);
    // oper.setStringFormat(prefix + ": " + format);
    return oper;
  }

  public static class KeyPartitionCodec<K, V> extends BaseKeyValueOperator.DefaultPartitionCodec<K,V> implements Serializable
  {
    private static final long serialVersionUID = 201410031623L;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    try {
      URI duri = PubSubHelper.getURIWithDefault(dag, "localhost:9090");

      PubSubWebSocketInputOperator userTxWsInput = getPubSubWebSocketInputOperator("userTxInput", dag, duri, "examples.app.frauddetect.submitTransaction");
      PubSubWebSocketOutputOperator ccUserAlertWsOutput = getPubSubWebSocketOutputOperator("ccUserAlertQueryOutput", dag, duri, "examples.app.frauddetect.fraudAlert");
      PubSubWebSocketOutputOperator avgUserAlertwsOutput = getPubSubWebSocketOutputOperator("avgUserAlertQueryOutput", dag, duri, "examples.app.frauddetect.fraudAlert");
      PubSubWebSocketOutputOperator binUserAlertwsOutput = getPubSubWebSocketOutputOperator("binUserAlertOutput", dag, duri, "examples.app.frauddetect.fraudAlert");
      PubSubWebSocketOutputOperator txSummaryWsOutput = getPubSubWebSocketOutputOperator("txSummaryWsOutput", dag, duri, "examples.app.frauddetect.txSummary");
      SlidingWindowSumKeyVal<KeyValPair<MerchantKey, String>, Integer> smsOperator = dag.addOperator("movingSum", SlidingWindowSumKeyVal.class);

      MerchantTransactionGenerator txReceiver = dag.addOperator("txReceiver", MerchantTransactionGenerator.class);
      MerchantTransactionInputHandler txInputHandler = dag.addOperator("txInputHandler", new MerchantTransactionInputHandler());
      BankIdNumberSamplerOperator binSampler = dag.addOperator("bankInfoFraudDetector", BankIdNumberSamplerOperator.class);

      MerchantTransactionBucketOperator txBucketOperator = dag.addOperator("txFilter", MerchantTransactionBucketOperator.class);
      RangeKeyVal rangeOperator = dag.addOperator("rangePerMerchant", new RangeKeyVal<MerchantKey, Long>());
      SimpleMovingAverage<MerchantKey, Long> smaOperator = dag.addOperator("smaPerMerchant", SimpleMovingAverage.class);
      TransactionStatsAggregator txStatsAggregator = dag.addOperator("txStatsAggregator", TransactionStatsAggregator.class);
      AverageAlertingOperator avgAlertingOperator = dag.addOperator("avgAlerter", AverageAlertingOperator.class);
      CreditCardAmountSamplerOperator ccSamplerOperator = dag.addOperator("amountFraudDetector", CreditCardAmountSamplerOperator.class);
      HdfsStringOutputOperator hdfsOutputOperator = getHdfsOutputOperator("hdfsOutput", dag, "fraud");

      MongoDBOutputOperator mongoTxStatsOperator = dag.addOperator("mongoTxStatsOutput", MongoDBOutputOperator.class);
      MongoDBOutputOperator mongoBinAlertsOperator = dag.addOperator("mongoBinAlertsOutput", MongoDBOutputOperator.class);
      MongoDBOutputOperator mongoCcAlertsOperator = dag.addOperator("mongoCcAlertsOutput", MongoDBOutputOperator.class);
      MongoDBOutputOperator mongoAvgAlertsOperator = dag.addOperator("mongoAvgAlertsOutput", MongoDBOutputOperator.class);

      dag.addStream("userTxStream", userTxWsInput.outputPort, txInputHandler.userTxInputPort);
      dag.addStream("transactions", txReceiver.txOutputPort, txBucketOperator.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("txData", txReceiver.txDataOutputPort, hdfsOutputOperator.input); // dump all tx into Hdfs
      dag.addStream("userTransactions", txInputHandler.txOutputPort, txBucketOperator.txUserInputPort);
      dag.addStream("bankInfoData", txBucketOperator.binCountOutputPort, smsOperator.data);
      dag.addStream("bankInfoCount", smsOperator.integerSum, binSampler.txCountInputPort);
      dag.addStream("filteredTransactions", txBucketOperator.txOutputPort, rangeOperator.data, smaOperator.data, avgAlertingOperator.txInputPort);

      KeyPartitionCodec<MerchantKey, Long> txCodec = new KeyPartitionCodec<MerchantKey, Long>();
      dag.setInputPortAttribute(rangeOperator.data, Context.PortContext.STREAM_CODEC, txCodec);
      dag.setInputPortAttribute(smaOperator.data, Context.PortContext.STREAM_CODEC, txCodec);
      dag.setInputPortAttribute(avgAlertingOperator.txInputPort, Context.PortContext.STREAM_CODEC, txCodec);

      dag.addStream("creditCardData", txBucketOperator.ccAlertOutputPort, ccSamplerOperator.inputPort);
      dag.addStream("txnSummaryData", txBucketOperator.summaryTxnOutputPort, txSummaryWsOutput.input);
      dag.addStream("smaAlerts", smaOperator.doubleSMA, avgAlertingOperator.smaInputPort);
      dag.addStream("binAlerts", binSampler.countAlertOutputPort, mongoBinAlertsOperator.inputPort);
      dag.addStream("binAlertsNotification", binSampler.countAlertNotificationPort, binUserAlertwsOutput.input);
      dag.addStream("rangeData", rangeOperator.range, txStatsAggregator.rangeInputPort);
      dag.addStream("smaData", smaOperator.longSMA, txStatsAggregator.smaInputPort);
      dag.addStream("txStatsOutput", txStatsAggregator.txDataOutputPort, mongoTxStatsOperator.inputPort);
      dag.addStream("avgAlerts", avgAlertingOperator.avgAlertOutputPort, mongoAvgAlertsOperator.inputPort);
      dag.addStream("avgAlertsNotification", avgAlertingOperator.avgAlertNotificationPort, avgUserAlertwsOutput.input);
      dag.addStream("ccAlerts", ccSamplerOperator.ccAlertOutputPort, mongoCcAlertsOperator.inputPort);
      dag.addStream("ccAlertsNotification", ccSamplerOperator.ccAlertNotificationPort, ccUserAlertWsOutput.input);

    } catch (Exception exc) {
      DTThrowable.rethrow(exc);
    }
  }
}
