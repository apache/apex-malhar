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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PojoFieldRetrieverExpression extends PojoFieldRetriever
{
  private static final Logger logger = LoggerFactory.getLogger(PojoFieldRetrieverExpression.class);

  private Map<String, String> fieldToExpression;

  public PojoFieldRetrieverExpression()
  {
  }

  @Override
  public void setup()
  {
    super.setup();

    for(Map.Entry<String, Type> entry: fieldToTypeInt.entrySet()) {
      String fieldName = entry.getKey();
      Type type = entry.getValue();

      logger.debug("fieldName {} type {}", fieldName, type);

      String expression = fieldToExpression.get(fieldName);

      switch(type)
      {
        case BOOLEAN:
        {
          if(fieldToGetterBoolean == null) {
            fieldToGetterBoolean = Maps.newHashMap();
          }

          fieldToGetterBoolean.put(fieldName,
          ConvertUtils.createExpressionGetterBoolean(getFQClassName(), expression));

          break;
        }
        case BYTE:
        {
          if(fieldToGetterByte == null) {
            fieldToGetterByte = Maps.newHashMap();
          }

          fieldToGetterByte.put(fieldName,
          ConvertUtils.createExpressionGetterByte(getFQClassName(), expression));

          break;
        }
        case CHAR:
        {
          if(fieldToGetterChar == null) {
            fieldToGetterChar = Maps.newHashMap();
          }

          fieldToGetterChar.put(fieldName,
          ConvertUtils.createExpressionGetterChar(getFQClassName(), expression));

          break;
        }
        case DOUBLE:
        {
          if(fieldToGetterDouble == null) {
            fieldToGetterDouble = Maps.newHashMap();
          }

          fieldToGetterDouble.put(fieldName,
          ConvertUtils.createExpressionGetterDouble(getFQClassName(), expression));

          break;
        }
        case FLOAT:
        {
          if(fieldToGetterFloat == null) {
            fieldToGetterFloat = Maps.newHashMap();
          }

          fieldToGetterFloat.put(fieldName,
          ConvertUtils.createExpressionGetterFloat(getFQClassName(), expression));

          break;
        }
        case INTEGER:
        {
          if(fieldToGetterInt == null) {
            fieldToGetterInt = Maps.newHashMap();
          }

          fieldToGetterInt.put(fieldName,
          ConvertUtils.createExpressionGetterInt(getFQClassName(), expression));

          break;
        }
        case LONG:
        {
          if(fieldToGetterLong == null) {
            fieldToGetterLong = Maps.newHashMap();
          }

          fieldToGetterLong.put(fieldName,
          ConvertUtils.createExpressionGetterLong(getFQClassName(), expression));

          break;
        }
        case SHORT:
        {
          if(fieldToGetterShort == null) {
            fieldToGetterShort = Maps.newHashMap();
          }

          fieldToGetterShort.put(fieldName,
          ConvertUtils.createExpressionGetterShort(getFQClassName(), expression));

          break;
        }
        case STRING:
        {
          if(fieldToGetterString == null) {
            fieldToGetterString = Maps.newHashMap();
          }

          fieldToGetterString.put(fieldName,
          ConvertUtils.createExpressionGetterString(getFQClassName(), expression));

          break;
        }
        case OBJECT:
        {
          if(fieldToGetterObject == null) {
            fieldToGetterObject = Maps.newHashMap();
          }

          fieldToGetterObject.put(fieldName,
          ConvertUtils.createExpressionGetterObject(getFQClassName(), expression));

          break;
        }
        default:
        {
          throw new UnsupportedOperationException("The type " + type + " is not supported.");
        }
      }
    }
  }

  public Map<String, String> getFieldToExpression()
  {
    return fieldToExpression;
  }

  public void setFieldToExpression(Map<String, String> fieldToExpression)
  {
    this.fieldToExpression = Preconditions.checkNotNull(fieldToExpression);

    for(Map.Entry<String, String> entry: fieldToExpression.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
    }
  }
}
