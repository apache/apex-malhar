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
package org.apache.apex.malhar.contrib.couchbase;

import java.util.ArrayList;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * An implementation of Couchbase Output Operator which takes a POJO,serializes it into key,value
 * pair and then writes to couchbase.
 *
 * @displayName Couchbase Output Operator
 * @category Output
 * @tags database, nosql, pojo, couchbase
 * @since 3.0.0
 */
@Evolving
public class CouchbasePOJOSetOperator extends AbstractCouchBaseSetOperator<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
  //Key stored in couchbase is always a string.
  private transient Getter<Object, String> keyGetter;
  //Value stored in Couchbase can be of these data types: boolean,numeric,string,arrays,object,null.
  private transient Getter<Object, ? extends Object> valueGetter;
  @NotNull
  private ArrayList<String> expressions;

  /*
   * An ArrayList of Java expressions that will yield the field value from the POJO.
   * Each expression corresponds to one column in the Couchbase table.
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }

  @Override
  public String getKey(Object tuple)
  {
    // first tuple
    if (null == keyGetter) {
      Class<?> tupleClass = tuple.getClass();
      keyGetter = PojoUtils.createGetter(tupleClass, expressions.get(0), String.class);
    }
    String key = keyGetter.get(tuple);
    logger.debug("key is {}", key);
    return key;
  }

  @Override
  public Object getValue(Object tuple)
  {
    Object value = null;
    if (null == valueGetter) {
      Class<?> tupleClass = tuple.getClass();
      final String getterExpression = expressions.get(1);
      valueGetter = PojoUtils.createGetter(tupleClass, getterExpression, Object.class);
    }
    if (valueGetter != null) {
      value = valueGetter.get(tuple);
    }
    logger.debug("value is {}", value);
    return value;
  }

}
