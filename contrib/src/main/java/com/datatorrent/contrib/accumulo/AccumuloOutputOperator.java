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
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.apache.accumulo.core.data.Mutation;

public class AccumuloOutputOperator extends AbstractAccumuloOutputOperator<Object>
{
  @NotNull
  private ArrayList<String> expressions;
  private transient ArrayList<Object> getters;
  @NotNull
 // private transient ArrayListfields;
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
    for(int i=0;i<size;i++)
    {
      if(expressions.get(i).equalsIgnoreCase("row"))
      {

      }

    }
    Text rowID = new Text("row1");
Text colFam = new Text("myColFam");
Text colQual = new Text("myColQual");
ColumnVisibility colVis = new ColumnVisibility("public");
long timestamp = System.currentTimeMillis();

Value value = new Value("myValue".getBytes());

Mutation mutation = new Mutation(rowID);
mutation.put(colFam, colQual, colVis, timestamp, value);

       mutation.put(t.getColFamily().getBytes(),t.getColName().getBytes(),t.getColValue().getBytes());
    return null;
  }

}
