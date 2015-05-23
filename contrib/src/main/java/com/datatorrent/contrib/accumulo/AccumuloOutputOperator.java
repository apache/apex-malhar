/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.accumulo;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import com.datatorrent.lib.util.PojoUtils.GetterString;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.validation.constraints.NotNull;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class AccumuloOutputOperator extends AbstractAccumuloOutputOperator<Object>
{
  @NotNull
  private ArrayList<String> expressions;
  private transient ArrayList<Object> getters;
  private transient ArrayList<String> dataTypes;

  public ArrayList<String> getDataTypes()
  {
    return dataTypes;
  }

  public void setDataTypes(ArrayList<String> dataTypes)
  {
    this.dataTypes = dataTypes;
  }

  @NotNull
  /*
   * An ArrayList of Java expressions that will yield the value of Accumulo column key from the POJO.
   * Each expression corresponds to a part in column key of accumulo key value store.
   */
  public ArrayList<String> getExpressions()
  {
    return expressions;
  }

  public void setExpressions(ArrayList<String> expressions)
  {
    this.expressions = expressions;
  }

  public AccumuloOutputOperator()
  {
    super();
    getters = new ArrayList<Object>();
    dataTypes = new ArrayList<String>();
  }

  public void processFirstTuple(Object tuple)
  {
    Class<?> fqcn = tuple.getClass();
    int size = expressions.size();
    Object getter;
    for (int i = 0; i < size; i++) {
      String getterExpression = PojoUtils.getSingleFieldExpression(fqcn, expressions.get(i));
      String type = dataTypes.get(i);
      if (type.equalsIgnoreCase("String")) {
        getter = PojoUtils.createGetterString(fqcn, getterExpression);
      }
      if (type.equalsIgnoreCase("Long")) {
        getter = PojoUtils.createExpressionGetterLong(fqcn, getterExpression);
      }
      else {
        getter = PojoUtils.createGetterObject(fqcn, getterExpression);
      }
      getters.add(getter);
    }

  }

  @Override
  public Mutation operationMutation(Object t)
  {
    if (getters.isEmpty()) {
      processFirstTuple(t);
    }
    int size = expressions.size();
    Text row = new Text(((GetterString)getters.get(0)).get(t));
    Mutation mutation = new Mutation(row);
    Text columnFamily = null;
    Text columnQualifier = null;
    ColumnVisibility columnVisibility = null;
    //Text columnValue = null;
    Long timestamp = null;
    Value value = null;
    for (int i = 1; i < size; i++) {
      if (expressions.get(i).equalsIgnoreCase("columnFamily")) {
        columnFamily = new Text(((GetterString)getters.get(i)).get(t));
      }
      if (expressions.get(i).equalsIgnoreCase("columnQualifier")) {
        columnQualifier = new Text(((GetterString)getters.get(i)).get(t));
      }
      if (expressions.get(i).equalsIgnoreCase("columnVisibility")) {
        String columnVis = (((GetterString)getters.get(i)).get(t));
        columnVisibility = new ColumnVisibility(columnVis);
      }
      if (expressions.get(i).equalsIgnoreCase("columnValue")) {
         String colValue = (((GetterString)getters.get(i)).get(t));
         value = new Value(colValue.getBytes());
      }
      if (expressions.get(i).equalsIgnoreCase("timestamp")) {
        timestamp = (((GetterLong)getters.get(i)).get(t));
      }

    }

    mutation.put(columnFamily, columnQualifier, columnVisibility, timestamp, value);

    return mutation;
  }

}
