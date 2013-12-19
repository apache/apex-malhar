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
package com.datatorrent.apps.telecom.operator;

import java.util.Map;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
*
* @since 0.9.2
*/
public class EnrichmentOperator<K,V,K1,V1> implements Operator
{

  /**
   * The concrete class that will enrich the incoming tuple
   */
  @NotNull
  private Class<? extends EnricherInterface<K,V,K1,V1>> enricher;
  /**
   * The properties that need to be configure enricher
   */
  @NotNull
  private Map<K,V> prop;
  
  /**
   * This is used to store the reference to enricher obj
   */
  private transient EnricherInterface<K,V,K1,V1> enricherObj;
  
  public final transient DefaultOutputPort<Map<K1,V1>> output = new DefaultOutputPort<Map<K1,V1>>();
  public final transient DefaultInputPort<Map<K1,V1>> input = new DefaultInputPort<Map<K1,V1>>() {
    @Override
    public void process(Map<K1,V1> t)
    {
      enricherObj.enrichRecord(t);
      output.emit(t);      
    }
  };
  
  @Override
  public void setup(OperatorContext context)
  {
    try {
      enricherObj = enricher.newInstance();
    } catch (Exception e) {
      logger.info("can't instantiate object {}", e.getMessage());
      throw new RuntimeException("setup failed");
    } 
    enricherObj.configure(prop);
  }

  @Override
  public void teardown()
  {
    
  }

  @Override
  public void beginWindow(long windowId)
  {
    
  }

  @Override
  public void endWindow()
  {
    
  }

  public Class<? extends EnricherInterface<K,V,K1,V1>> getEnricher()
  {
    return enricher;
  }

  public void setEnricher(Class<? extends EnricherInterface<K,V,K1,V1>> enricher)
  {
    this.enricher = enricher;
  }

  public Map<K,V> getProp()
  {
    return prop;
  }

  public void setProp(Map<K,V> prop)
  {
    this.prop = prop;
  }

  private static final Logger logger = LoggerFactory.getLogger(EnrichmentOperator.class);
  
}
