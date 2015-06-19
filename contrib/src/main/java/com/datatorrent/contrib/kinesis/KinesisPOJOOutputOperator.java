/**
 * 
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
 *
 */
package com.datatorrent.contrib.kinesis;

import java.util.List;

import com.datatorrent.common.util.Pair;
import com.datatorrent.contrib.util.FieldInfo;
import com.datatorrent.contrib.util.FieldValueGenerator;
import com.datatorrent.contrib.util.TableInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

public class KinesisPOJOOutputOperator extends AbstractKinesisOutputOperator<byte[], Object>
{
  private TableInfo<FieldInfo> tableInfo;

  private transient FieldValueGenerator<FieldInfo> fieldValueGenerator;

  private transient Getter<Object, String> rowGetter;
  
  @Override
  protected byte[] getRecord(byte[] value)
  {
    return value;
  }

  @Override
  protected Pair<String, byte[]> tupleToKeyValue(Object tuple)
  {
    // key
    if (rowGetter == null) 
    {
      // use string as row id
      rowGetter = PojoUtils.createGetter(tuple.getClass(), tableInfo.getRowOrIdExpression(), String.class);
    }
    String key = rowGetter.get( tuple );
    
    //value
    final List<FieldInfo> fieldsInfo = tableInfo.getFieldsInfo();
    if (fieldValueGenerator == null) {
      fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(tuple.getClass(), fieldsInfo);
    }
    return new Pair< String, byte[]>( key, fieldValueGenerator.serializeObject(tuple) );
  }


  /**
   * Table Information
   */
  public TableInfo<FieldInfo> getTableInfo()
  {
    return tableInfo;
  }

  /**
   * Table Information
   */
  public void setTableInfo(TableInfo<FieldInfo> tableInfo)
  {
    this.tableInfo = tableInfo;
  }

}
