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
import com.datatorrent.lib.util.PojoUtils.Getter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import javax.validation.constraints.NotNull;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/*
 * An implementation of AbstractAccumuloOutputOperator which gets data types of key columns and value column
 * from user and creates a mutation to be stored in Accumulo table accordingly.
 */
public class AccumuloOutputOperator extends AbstractAccumuloOutputOperator<Object>
{
  @NotNull
  private ArrayList<String> expressions;
  private transient Getter<Object,String> rowGetter;
  private transient Getter<Object,String> colFamilyGetter;
  private transient Getter<Object,String> columnQualifierGetter;
  private transient Getter<Object,String> columnValueGetter;
  private transient Getter<Object,Long> timestampGetter;
  private transient Getter<Object,String> columnVisibilityGetter;

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


  @Override
  @SuppressWarnings("unchecked")
  public Mutation operationMutation(Object t)
  {
    if (null == rowGetter) {
      Class<?> fqcn = t.getClass();
      rowGetter = PojoUtils.createGetter(fqcn, expressions.get(0),String.class);
      colFamilyGetter = PojoUtils.createGetter(fqcn, expressions.get(1),String.class);
      columnQualifierGetter = PojoUtils.createGetter(fqcn, expressions.get(2),String.class);
      columnVisibilityGetter = PojoUtils.createGetter(fqcn, expressions.get(3), String.class);
      columnValueGetter = PojoUtils.createGetter(fqcn, expressions.get(4), String.class);
      timestampGetter = PojoUtils.createGetter(fqcn, expressions.get(5), Long.class);
    }

    Text row = new Text(rowGetter.get(t));
    Mutation mutation = new Mutation(row);
    Text columnFamily = new Text(colFamilyGetter.get(t));
    Text columnQualifier = new Text(columnQualifierGetter.get(t));
    ColumnVisibility columnVisibility = new ColumnVisibility(columnVisibilityGetter.get(t));
    Value value = new Value(columnValueGetter.get(t).getBytes());
    Long timestamp = timestampGetter.get(t);

    mutation.put(columnFamily, columnQualifier, columnVisibility, timestamp, value);

    return mutation;
  }

}

