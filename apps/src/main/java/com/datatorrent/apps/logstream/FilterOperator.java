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
package com.datatorrent.apps.logstream;

import java.util.*;
import java.util.HashMap;
import java.util.Map;

import javax.validation.ValidationException;

import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;

import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.apps.logstream.PropertyRegistry.PropertyRegistry;
import com.datatorrent.common.util.DTThrowable;

/**
 *
 * Filters an input tuple and emits a filter stamped tuple for each satisfied filter
 */
@ShipContainingJars(classes = {org.codehaus.janino.ExpressionEvaluator.class})
public class FilterOperator extends BaseOperator
{
  public final transient DefaultOutputPort<HashMap<String, Object>> outputMap = new DefaultOutputPort<HashMap<String, Object>>();
  public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
  {
    @Override
    public void process(Map<String, Object> map)
    {
      HashMap<String, Object> filterTuple;
      int typeId = (Integer)map.get(LogstreamUtil.LOG_TYPE);
      Map<String, String[]> conditions = conditionList.get(typeId);
      if (conditions != null) {
        for (String condition : conditions.keySet()) {
          if (evaluate(condition, map, conditions.get(condition))) {
            int index = registry.getIndex(LogstreamUtil.FILTER, condition);
            filterTuple = new HashMap<String, Object>(map);
            filterTuple.put(LogstreamUtil.FILTER, index);
            outputMap.emit(filterTuple);
          }
        }
      }

      // emit the same tuple for default condition
      int defaultFilterIndex = registry.getIndex(LogstreamUtil.FILTER, registry.lookupValue(typeId) + "_" + "DEFAULT");

      if (defaultFilterIndex >= 0) {
        map.put(LogstreamUtil.FILTER, defaultFilterIndex);
        outputMap.emit((HashMap<String, Object>)map);
      }
    }

    private boolean evaluate(String condition, Map<String, Object> map, String[] keys)
    {
      boolean ret = false;

      Object[] values = new Object[keys.length];
      Class[] classTypes = new Class[keys.length];
      ExpressionEvaluator ee = evaluators.get(condition);

      try {
        for (int i = 0; i < keys.length; i++) {
          Object val = map.get(keys[i]);

          if (val == null) {
            logger.debug("filter key {} missing in input record {}", keys[i], map);
            return ret;
          }
          else {
            values[i] = val;
            classTypes[i] = val.getClass();
          }
        }

        if (ee == null) {
          ee = new ExpressionEvaluator(condition, Boolean.class, keys, classTypes);
          evaluators.put(condition, ee);
        }

        ret = (Boolean)ee.evaluate(values);
        logger.debug("expression evaluated to {} for expression: {} with key class types: {} keys: {} values: {}", ret, condition, Arrays.toString(classTypes), Arrays.toString(keys), Arrays.toString(values));

      }
      catch (Throwable t) {
        DTThrowable.rethrow(t);
      }

      return ret;
    }

  };
  private static final Logger logger = LoggerFactory.getLogger(FilterOperator.class);
  /**
   * key: type
   * value --> map of
   * key: condition expression
   * value: list of keys on which the condition is
   */
  private HashMap<Integer, Map<String, String[]>> conditionList = new HashMap<Integer, Map<String, String[]>>();
  private transient HashMap<String, ExpressionEvaluator> evaluators = new HashMap<String, ExpressionEvaluator>();
  private PropertyRegistry<String> registry;

  /**
   * supply the registry object which is used to store and retrieve meta information about each tuple
   *
   * @param registry
   */
  public void setRegistry(PropertyRegistry<String> registry)
  {
    this.registry = registry;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  /**
   * Supply the properties to the operator.
   * Input includes following properties:
   * type=logtype // input logtype for which the properties are to be set
   * followed by list of keys followed by the filter expression
   * eg: type=apache, response, response.equals("404")
   *
   * if default filter is needed i.e. emit a tuple with no filter applied on it
   * Input includes following properties:
   * type=logtype // input logtype for which the properties are to be set
   * followed by the following default condition
   * default=true
   * eg: type=apache, default=true
   *
   * @param properties
   */
  public void addFilterCondition(String[] properties)
  {
    try {
      // TODO: validations
      if (properties.length == 2) {
        logger.debug(Arrays.toString(properties));
        String[] split = properties[0].split("=");
        String type = split[1];
        String[] split1 = properties[1].split("=");
        if (split1[1].toLowerCase().equals("true")) {
          registry.bind(LogstreamUtil.FILTER, type + "_" + "DEFAULT");
        }
      }
      else if (properties.length > 2) {
        String[] split = properties[0].split("=");
        String type = split[1];
        int typeId = registry.getIndex(LogstreamUtil.LOG_TYPE, type);
        String[] keys = new String[properties.length - 2];

        System.arraycopy(properties, 1, keys, 0, keys.length);

        String expression = properties[properties.length - 1];

        Map<String, String[]> conditions = conditionList.get(typeId);
        if (conditions == null) {
          conditions = new HashMap<String, String[]>();
          conditionList.put(typeId, conditions);
        }

        conditions.put(expression, keys);
        if (registry != null) {
          registry.bind(LogstreamUtil.FILTER, expression);
        }
      }
      else {
        throw new ValidationException("Invalid input property string " + Arrays.toString(properties));
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
