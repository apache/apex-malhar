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
package com.datatorrent.contrib.geode;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.TableInfo;

/**
 *
 * @displayName Geode Output Operator
 * @category Output
 * @tags pojo, geode
 * 
 *
 * @since 3.4.0
 */
@Evolving
public class GeodePOJOOutputOperator extends AbstractGeodeOutputOperator<Object>
{

  private TableInfo<FieldInfo> tableInfo;
  private transient Getter<Object, String> rowGetter;

  @Override
  public void processTuple(Object tuple)
  {
    if (rowGetter == null) {
      rowGetter = PojoUtils.createGetter(tuple.getClass(), tableInfo.getRowOrIdExpression(), String.class);
    }

    getStore().put(rowGetter.get(tuple), tuple);
  }

  /**
   *
   * the information to convert pojo
   */
  public TableInfo<FieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  /**
   *
   * the information to convert pojo
   */
  public void setTableInfo(TableInfo<FieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

}
