/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchbase;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import java.lang.reflect.Array;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An implementation of Couchbase Output Operator which takes a POJO,serializes it into key,value
 * pair and then writes to couchbase.
 */
public class CouchbasePOJOOutputOperator extends AbstractCouchBaseSetOperator<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseStore.class);
  //Key stored in couchbase is always a string.
  private transient Getter<Object, String> keyGetter;
  //Value stored in Couchbase can be of these data types: boolean,numeric,string,arrays,object,null.
  private transient Getter<Object, Object> valueGetter;
  @NotNull
  private ArrayList<String> expressions;
  @NotNull
  private FIELD_TYPE type;

  public FIELD_TYPE getType()
  {
    return type;
  }

  public void setType(FIELD_TYPE type)
  {
    this.type = type;
  }

  public enum FIELD_TYPE
  {
    BOOLEAN, NUMBER, STRING, ARRAY, OBJECT, NULL
  };

  /*
   * An ArrayList of Java expressions that will yield the field value from the POJO.
   * Each expression corresponds to one column in the Cassandra table.
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
      switch (type) {
        case NUMBER:
          Getter<Object, Number> numberGetter = PojoUtils.createGetter(tupleClass, getterExpression, Number.class);
          value = numberGetter.get(tuple);
          break;
        case STRING:
          Getter<Object, String> stringGetter = PojoUtils.createGetter(tupleClass, getterExpression, String.class);
          value = stringGetter.get(tuple);
          break;
        case BOOLEAN:
          GetterBoolean<Object> booleanGetter = PojoUtils.createGetterBoolean(tupleClass, getterExpression);
          value = booleanGetter.get(tuple);
          break;
        case ARRAY:
          Getter<Object, Array> getterArray = PojoUtils.createGetter(tupleClass, getterExpression, Array.class);
          value = getterArray.get(tuple);
          break;
        case OBJECT:
          Getter<Object, Object> getterObject = PojoUtils.createGetter(tupleClass, getterExpression, Object.class);
          value = getterObject.get(tuple);
          break;
        case NULL:
          value = null;
          break;
        default:
          throw new RuntimeException("unsupported data type " + type);
      }

    }
    logger.debug("value is {}", value);
    return value;
  }

}
