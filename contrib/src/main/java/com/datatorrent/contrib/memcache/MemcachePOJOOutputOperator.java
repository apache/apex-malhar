/**
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
package com.datatorrent.contrib.memcache;

import java.util.List;
import java.util.Map;

import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldValueGenerator;
import com.datatorrent.lib.util.TableInfo;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;

/**
 * 
 * @displayName Memcache Output Operator
 * @category Database
 * @tags Memcache, output operator, Pojo
 */
public class MemcachePOJOOutputOperator extends AbstractMemcacheOutputOperator< Object >
{
  private static final long serialVersionUID = 5290158463990158290L;
  private TableInfo<FieldInfo> tableInfo;
  private transient FieldValueGenerator<FieldInfo> fieldValueGenerator;
  private transient Getter<Object, String> rowGetter;
  
  @Override
  public void processTuple(Object tuple)
  {
    if (rowGetter == null) {
      // use string as row id
      rowGetter = PojoUtils.createGetter(tuple.getClass(), tableInfo.getRowOrIdExpression(), String.class);
    }

    final List<FieldInfo> fieldsInfo = tableInfo.getFieldsInfo();
    Object value = tuple;
    if( fieldsInfo != null )
    {
      if (fieldValueGenerator == null) {
        fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(tuple.getClass(), fieldsInfo);
      }
      
      Map<String, Object> fieldValue = fieldValueGenerator.getFieldsValueAsMap(tuple);
    }
   
    getStore().put( rowGetter.get(tuple), value);
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
