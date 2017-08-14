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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.examples.frauddetect.util.JsonUtils;
import org.apache.apex.malhar.lib.util.HighLow;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Operator to aggregate the min, max, sma, std-dev and variance for the given key.
 *
 * @since 0.9.0
 */
public class TransactionStatsAggregator extends BaseOperator
{
  public Map<MerchantKey, TransactionStatsData> aggrgateMap =
      new HashMap<MerchantKey, TransactionStatsData>();
  public final transient DefaultOutputPort<String> txDataOutputPort = new DefaultOutputPort<String>();
  public final transient DefaultInputPort<KeyValPair<MerchantKey, HighLow<Long>>> rangeInputPort =
      new DefaultInputPort<KeyValPair<MerchantKey, HighLow<Long>>>()
  {
    @Override
    public void process(KeyValPair<MerchantKey, HighLow<Long>> tuple)
    {
      TransactionStatsData data = getDataObjectFromMap(tuple.getKey());
      // HighLow is not currently typed, casting till it is fixed
      data.min = tuple.getValue().getLow();
      data.max = tuple.getValue().getHigh();
    }

  };
  public final transient DefaultInputPort<KeyValPair<MerchantKey, Long>> smaInputPort =
      new DefaultInputPort<KeyValPair<MerchantKey, Long>>()
  {
    @Override
    public void process(KeyValPair<MerchantKey, Long> tuple)
    {
      TransactionStatsData data = getDataObjectFromMap(tuple.getKey());
      data.sma = tuple.getValue();
    }

  };

  private TransactionStatsData getDataObjectFromMap(MerchantKey key)
  {
    TransactionStatsData data = aggrgateMap.get(key);
    if (data == null) {
      data = new TransactionStatsData();
      data.time = System.currentTimeMillis();
      data.merchantId = key.merchantId;
      data.terminalId = key.terminalId == null ? 0 : key.terminalId;
      data.zipCode = key.zipCode;
      data.merchantType = key.merchantType;
      aggrgateMap.put(key, data);
    }
    return data;
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MerchantKey, TransactionStatsData> entry : aggrgateMap.entrySet()) {
      try {
        txDataOutputPort.emit(JsonUtils.toJson(entry.getValue()));
      } catch (IOException e) {
        logger.warn("Exception while converting object to JSON", e);
      }
    }
    aggrgateMap.clear();
  }

  private static final Logger logger = LoggerFactory.getLogger(TransactionStatsAggregator.class);
}
