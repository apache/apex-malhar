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
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

public class AccumuloOutputOperator extends AbstractAccumuloOutputOperator<Object>
{
  @NotNull
  private ArrayList<String> expressions;
  private transient ArrayList<Object> getters;

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
  }

  public void processFirstTuple(Object tuple)
  {
    Class<?> fqcn = tuple.getClass();
    int size = expressions.size();
    for (int i = 0; i < size; i++) {
      String getterExpression = PojoUtils.getSingleFieldExpression(fqcn, expressions.get(i));

      GetterObject getObject = PojoUtils.createGetterObject(fqcn, getterExpression);
      getters.add(getObject);
    }

  }

  @Override
  public Mutation operationMutation(Object t)
  {
    if (getters.isEmpty()) {
      processFirstTuple(t);
    }
    int size = expressions.size();
    byte[] row = (byte[])(((GetterObject)getters.get(0)).get(t));
    Mutation mutation = new Mutation(row);
    byte[] columnFamily = null;
    byte[] columnQualifier = null;
    ColumnVisibility columnVisibility = null;
    byte[] columnValue = null;
    Long timestamp = null;
    byte[] value = null;
    for (int i = 1; i < size; i++) {
      if (expressions.get(i).equalsIgnoreCase("columnFamily")) {
        columnFamily = (byte[])(((GetterObject)getters.get(i)).get(t));
      }
      if (expressions.get(i).equalsIgnoreCase("columnQualifier")) {
        columnQualifier = (byte[])(((GetterObject)getters.get(i)).get(t));
      }
      if (expressions.get(i).equalsIgnoreCase("columnVisibility")) {
        byte[] columnVis = (byte[])(((GetterObject)getters.get(i)).get(t));
        columnVisibility = new ColumnVisibility(columnVis);
      }
      if (expressions.get(i).equalsIgnoreCase("columnValue")) {
        columnValue = (byte[])(((GetterObject)getters.get(i)).get(t));
      }
      if (expressions.get(i).equalsIgnoreCase("timestamp")) {
        timestamp = (((GetterLong)getters.get(i)).get(t));
      }

      if (expressions.get(i).equalsIgnoreCase("timestamp")) {
        timestamp = (((GetterLong)getters.get(i)).get(t));
      }

    }

    mutation.put(columnFamily, columnQualifier, columnVisibility, timestamp, columnValue);

    return mutation;
  }

}
