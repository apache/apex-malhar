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
package com.datatorrent.apps.telecom.cdr.simulator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;

import com.datatorrent.apps.telecom.cdr.simulator.CDRGenRegistry.UK_Standard_2012;
import com.google.common.base.Function;

/**
 * A CDR records simulator used to generate data to either kafka or file system(not implemented yet)
 */
public class CDRSimulator
{
  private static final Random rand = new Random();
  
  private static final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
  
  public static void main(String[] args) throws IOException{
    
    
    
    System.out.print("Output(kafka/file/hdfs): ");

    String line = br.readLine();
    System.out.println();
    if (line.equals("kafka")) {
      kafkaOutput();
    } else {
      
    }
    
  }

  /**
   * 
   * Output records to Kafka
   * @throws IOException
   */
  private static void kafkaOutput() throws IOException
  {
    System.out.print("Broker: ");
    String brokerList = br.readLine();
    System.out.println();
    System.out.print("Topic: ");
    final String topic = br.readLine();
    System.out.println();
    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", TypePartitioner.class.getCanonicalName());
    props.put("producer.type", "async");
    props.put("batch.num.messages", "100");
    ProducerConfig config = new ProducerConfig(props);
    
    ScheduledExecutorService service = Executors.newScheduledThreadPool(2);
    for (int i = 0; i < 2; i++) {
      final Producer<String, String> producer = new Producer<String, String>(config);
      service.scheduleAtFixedRate(new Runnable() {

        @Override
        public void run()
        {
          
          for (int k = 0; k < 20000; k++) {
            Object[] tuple = dataFromTemplate(UK_Standard_2012.dataTemplate);
            String msg = "\"" + StringUtils.join(dataFromTemplate(UK_Standard_2012.dataTemplate), "\",\"") + "\"";
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, tuple[0].toString(), msg);
            producer.send(data);
          }

        }
      }, 0, 1000, TimeUnit.MILLISECONDS);
    }
    
    
  }

  /**
   * Generate value from the template
   * 
   * @param dataTemplate
   * @return
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static String[] dataFromTemplate(Object[] dataTemplate)
  {
    String[] data = new String[dataTemplate.length];
    int i = 0;
    for (Object template : dataTemplate) {
      if(template.getClass().isArray()){
        Object[] ta = (Object[])template;
        if(ta[0] instanceof Function){
          data[i++] = ((Function)ta[0]).apply(ta[1]).toString();
        } else {
          data[i++] = ta[rand.nextInt(ta.length)].toString();
        }
      } else {
        data[i++] = template.toString();
      }
    }
    return data;
  }
  
  

}
