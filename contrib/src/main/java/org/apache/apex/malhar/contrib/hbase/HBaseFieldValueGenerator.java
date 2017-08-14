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
package org.apache.apex.malhar.contrib.hbase;

import java.util.List;

import org.apache.apex.malhar.lib.util.FieldValueGenerator;
import org.apache.apex.malhar.lib.util.PojoUtils;

/**
 * A {@link FieldValueGenerator} implementation for {@link HBaseFieldInfo}
 *
 * @since 3.5.0
 */
public class HBaseFieldValueGenerator extends FieldValueGenerator<HBaseFieldInfo>
{
  public static final String COLON = ":";

  @SuppressWarnings("unchecked")
  protected HBaseFieldValueGenerator(final Class<?> clazz, List<HBaseFieldInfo> fieldInfos)
  {
    for (HBaseFieldInfo fieldInfo : fieldInfos) {
      fieldInfoMap.put(fieldInfo.getFamilyName() + COLON + fieldInfo.getColumnName(), fieldInfo);

      PojoUtils.Getter<Object, Object> getter =
          PojoUtils.createGetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      fieldGetterMap.put(fieldInfo, getter);
    }

    for (HBaseFieldInfo fieldInfo : fieldInfos) {
      PojoUtils.Setter<Object, Object> setter =
          PojoUtils.createSetter(clazz, fieldInfo.getPojoFieldExpression(), fieldInfo.getType().getJavaType());
      fieldSetterMap.put(fieldInfo, setter);
    }
  }

  public void setColumnValue(Object instance, String columnName, String columnFamily, Object value,
      ValueConverter<HBaseFieldInfo> valueConverter)
  {
    HBaseFieldInfo fieldInfo = fieldInfoMap.get(columnFamily + COLON + columnName);
    PojoUtils.Setter<Object, Object> setter = fieldSetterMap.get(fieldInfo);
    setter.set(instance, valueConverter == null ? value : valueConverter.convertValue(fieldInfo, value));
  }

}
