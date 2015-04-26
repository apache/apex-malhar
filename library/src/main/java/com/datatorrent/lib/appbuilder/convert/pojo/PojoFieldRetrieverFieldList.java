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

package com.datatorrent.lib.appbuilder.convert.pojo;

import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Map;

public class PojoFieldRetrieverFieldList extends PojoFieldRetriever
{
  private Map<String, ArrayList<String>> fieldToFieldList;

  public PojoFieldRetrieverFieldList()
  {
  }

  @Override
  public void setup()
  {
    for(Map.Entry<String, Type> entry: getFieldToType().entrySet()) {
      String fieldName = entry.getKey();
      Type type = entry.getValue();

      ArrayList<String> fieldList = fieldToFieldList.get(fieldName);

      switch(type)
      {
        case BOOLEAN:
        {
          if(fieldToGetterBoolean == null) {
            fieldToGetterBoolean = Maps.newHashMap();
          }

          fieldToGetterBoolean.put(fieldName,
          ConvertUtils.createExpressionGetterBoolean(getFQClassName(), fieldList));

          break;
        }
        case BYTE:
        {
          if(fieldToGetterByte == null) {
            fieldToGetterByte = Maps.newHashMap();
          }

          fieldToGetterByte.put(fieldName,
          ConvertUtils.createExpressionGetterByte(getFQClassName(), fieldList));

          break;
        }
        case CHAR:
        {
          if(fieldToGetterChar == null) {
            fieldToGetterChar = Maps.newHashMap();
          }

          fieldToGetterChar.put(fieldName,
          ConvertUtils.createExpressionGetterChar(getFQClassName(), fieldList));

          break;
        }
        case DOUBLE:
        {
          if(fieldToGetterDouble == null) {
            fieldToGetterDouble = Maps.newHashMap();
          }

          fieldToGetterDouble.put(fieldName,
          ConvertUtils.createExpressionGetterDouble(getFQClassName(), fieldList));

          break;
        }
        case FLOAT:
        {
          if(fieldToGetterFloat == null) {
            fieldToGetterFloat = Maps.newHashMap();
          }

          fieldToGetterFloat.put(fieldName,
          ConvertUtils.createExpressionGetterFloat(getFQClassName(), fieldList));

          break;
        }
        case INTEGER:
        {
          if(fieldToGetterInt == null) {
            fieldToGetterInt = Maps.newHashMap();
          }

          fieldToGetterInt.put(fieldName,
          ConvertUtils.createExpressionGetterInt(getFQClassName(), fieldList));

          break;
        }
        case LONG:
        {
          if(fieldToGetterLong == null) {
            fieldToGetterLong = Maps.newHashMap();
          }

          fieldToGetterLong.put(fieldName,
          ConvertUtils.createExpressionGetterLong(getFQClassName(), fieldList));

          break;
        }
        case SHORT:
        {
          if(fieldToGetterShort == null) {
            fieldToGetterShort = Maps.newHashMap();
          }

          fieldToGetterShort.put(fieldName,
          ConvertUtils.createExpressionGetterShort(getFQClassName(), fieldList));

          break;
        }
        case STRING:
        {
          if(fieldToGetterString == null) {
            fieldToGetterString = Maps.newHashMap();
          }

          fieldToGetterString.put(fieldName,
          ConvertUtils.createExpressionGetterString(getFQClassName(), fieldList));

          break;
        }
        case OBJECT:
        {
          if(fieldToGetterObject == null) {
            fieldToGetterObject = Maps.newHashMap();
          }

          fieldToGetterObject.put(fieldName,
          ConvertUtils.createExpressionGetterObject(getFQClassName(), fieldList));

          break;
        }
        default:
        {
          throw new UnsupportedOperationException("The type " + type + " is not supported.");
        }
      }
    }
  }
}
