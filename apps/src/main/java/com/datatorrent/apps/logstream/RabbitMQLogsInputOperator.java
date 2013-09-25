/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.logstream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.rabbitmq.AbstractSinglePortRabbitMQInputOperator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *
 * Input operator to consume logs messages from RabbitMQ
 */
public class RabbitMQLogsInputOperator extends AbstractSinglePortRabbitMQInputOperator<Map<String, Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQLogsInputOperator.class);

  @Override
  public Map<String,Object> getTuple(byte[] message)
  {
    String inputString = new String(message);
    try {
      JSONObject jSONObject = new JSONObject(inputString);
      Iterator<String> iterator = jSONObject.keys();
      Map<String,Object> map = new HashMap<String, Object>();
      while(iterator.hasNext()){
        String key = iterator.next();
        map.put(key, jSONObject.getString(key));
      }

      return map;
    }
    catch (JSONException ex) {
      logger.error(ex.getMessage());
    }
    return null;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    logger.info("exchange = {}",this.getExchange());
    logger.info("host name = {}", this.getHost());
    logger.info("exchange type = {}", this.getExchangeType());
  }

}
