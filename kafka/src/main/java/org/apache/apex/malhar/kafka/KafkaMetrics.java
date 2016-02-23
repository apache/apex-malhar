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
package org.apache.apex.malhar.kafka;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.datatorrent.api.AutoMetric;

/**
 * Metrics class
 *
 * @since 3.3.0
 */
@InterfaceStability.Evolving
public class KafkaMetrics implements Serializable
{
  private KafkaConsumerStats[] stats;

  private transient long lastMetricSampleTime = 0L;

  private transient long metricsRefreshInterval;

  public KafkaMetrics(long metricsRefreshInterval)
  {
    this.metricsRefreshInterval = metricsRefreshInterval;
  }

  void updateMetrics(String[] clusters, Map<String, Map<MetricName, ? extends Metric>> metricsMap)
  {
    long current = System.currentTimeMillis();
    if (current - lastMetricSampleTime < metricsRefreshInterval) {
      return;
    }

    lastMetricSampleTime = current;

    if (stats == null) {
      stats = new KafkaConsumerStats[clusters.length];
    }

    for (int i = 0; i < clusters.length; i++) {
      if (stats[i] == null) {
        stats[i] = new KafkaConsumerStats();
        stats[i].cluster = clusters[i];
      }
      Map<MetricName, ? extends Metric> cMetrics = metricsMap.get(clusters[i]);
      if (cMetrics == null || cMetrics.isEmpty()) {
        stats[i].bytesPerSec = 0;
        stats[i].msgsPerSec = 0;
        continue;
      }
      if (stats[i].bytePerSecMK == null || stats[i].msgPerSecMK == null) {
        for (MetricName mn : cMetrics.keySet()) {
          if (mn.name().equals("bytes-consumed-rate")) {
            stats[i].bytePerSecMK = mn;
          } else if (mn.name().equals("records-consumed-rate")) {
            stats[i].msgPerSecMK = mn;
          }
        }
      }
      stats[i].bytesPerSec = cMetrics.get(stats[i].bytePerSecMK).value();
      stats[i].msgsPerSec = cMetrics.get(stats[i].msgPerSecMK).value();
    }
  }

  public KafkaConsumerStats[] getStats()
  {
    return stats;
  }

  /**
   * Counter class which gives the statistic value from the consumer
   */
  public static class KafkaConsumerStats implements Serializable
  {
    private static final long serialVersionUID = -2867402654990209006L;

    public transient MetricName msgPerSecMK;
    public transient MetricName bytePerSecMK;

    public String cluster;
    /**
     * Metrics for each consumer
     */
    public double msgsPerSec;

    public double bytesPerSec;

    public KafkaConsumerStats()
    {
    }
  }

  public static class KafkaMetricsAggregator implements AutoMetric.Aggregator, Serializable
  {

    @Override
    public Map<String, Object> aggregate(long l, Collection<AutoMetric.PhysicalMetricsContext> collection)
    {
      double totalBytesPerSec = 0;
      double totalMsgsPerSec = 0;
      Map<String, Object> total = new HashMap<>();
      for (AutoMetric.PhysicalMetricsContext pmc : collection) {
        KafkaMetrics km = (KafkaMetrics)pmc.getMetrics().get("metrics");
        for (KafkaConsumerStats kcs : km.stats) {
          totalBytesPerSec += kcs.bytesPerSec;
          totalMsgsPerSec += kcs.msgsPerSec;
        }
      }
      total.put("totalBytesPerSec", totalBytesPerSec);
      total.put("totalMsgPerSec", totalMsgsPerSec);
      return total;
    }
  }
}


