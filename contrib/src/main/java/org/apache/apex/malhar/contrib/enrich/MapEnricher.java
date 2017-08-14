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
package org.apache.apex.malhar.contrib.enrich;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This class takes a HashMap tuple as input and extracts value of the lookupKey configured
 * for this operator. It perform a lookup using {@link org.apache.apex.malhar.lib.db.cache.CacheManager} to
 * find a matching entry and adds the result to the original tuple.
 *
 * <p>
 * Example:
 * Lets say, input tuple is
 *    { amount=10.0, channelId=4, productId=3 }
 * The tuple is modified as below:
 *    { amount=10.0, channelId=4, productId=3, <b>productCategory=1 </b>}
 * </p>
 *
 * @displayName MapEnricher
 * @category Database
 * @tags enrichment, lookup, map
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class MapEnricher extends AbstractEnricher<Map<String, Object>, Map<String, Object>>
{
  public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> obj)
    {
      processTuple(obj);
    }
  };

  public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<>();

  protected void processTuple(Map<String, Object> obj)
  {
    enrichTuple(obj);
  }

  @Override
  protected Object getKey(Map<String, Object> tuple)
  {
    ArrayList<Object> keyList = new ArrayList<Object>();

    for (FieldInfo fieldInfo : lookupFieldInfo) {
      keyList.add(tuple.get(fieldInfo.getColumnName()));
    }

    return keyList;
  }

  @Override
  protected Map<String, Object> convert(Map<String, Object> in, Object cached)
  {
    if (cached == null) {
      return in;
    }

    ArrayList<Object> newAttributes = (ArrayList<Object>)cached;
    if (newAttributes != null) {
      for (int i = 0; i < includeFieldInfo.size(); i++) {
        in.put(includeFieldInfo.get(i).getColumnName(), newAttributes.get(i));
      }
    }
    return in;
  }

  @Override
  protected void emitEnrichedTuple(Map<String, Object> tuple)
  {
    output.emit(tuple);
  }

  @Override
  protected Class<?> getIncludeFieldType(String fieldName)
  {
    return Object.class;
  }

  @Override
  protected Class<?> getLookupFieldType(String fieldName)
  {
    return Object.class;
  }

  /**
   * Set fields on which lookup against which lookup will be performed.
   * This is a mandatory parameter to set.
   *
   * @param lookupFields List of fields on which lookup happens.
   * @description $[] Field which become part of lookup key
   */
  @Override
  public void setLookupFields(List<String> lookupFields)
  {
    super.setLookupFields(lookupFields);
  }

  /**
   * Set fields which will enrich the map.
   * This is a mandatory parameter to set.
   *
   * @param includeFields List of fields.
   * @description $[] Field which are fetched from store
   */
  @Override
  public void setIncludeFields(List<String> includeFields)
  {
    super.setIncludeFields(includeFields);
  }
}
