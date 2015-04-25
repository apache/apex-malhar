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

import com.google.common.base.Preconditions;

import java.util.List;

public class ConvertUtils
{
  public static final String JAVA_DOT = ".";
  public static final String DEFAULT_TEMP_POJO_NAME = "dt_pojo";
  public static final String DEFAULT_POJO_NAME = "pojo";

  public static final String GET = "get";
  public static final String IS = "is";

  private ConvertUtils()
  {
  }

  public static String upperCaseWord(String field)
  {
    Preconditions.checkArgument(!field.isEmpty(), field);
    return field.substring(0, 1).toUpperCase() + field.substring(1);
  }

  public static String getFieldGetter(String field)
  {
    return GET + upperCaseWord(field);
  }

  public static String getBooleanGetter(String field)
  {
    return IS + upperCaseWord(field);
  }

  public static String getFieldGetter(String field, boolean isBoolean)
  {
    if(isBoolean) {
      return getBooleanGetter(field);
    }
    else {
      return getFieldGetter(field);
    }
  }

  public static String fieldListToGetExpression(List<String> fields, boolean isBoolean)
  {
    StringBuilder sb = new StringBuilder();

    for(int index = 0;
        index < fields.size() - 1;
        index++) {
      String field = fields.get(index);
      sb.append(sb).append(getFieldGetter(field)).append(JAVA_DOT);
    }

    sb.append(getFieldGetter(fields.get(fields.size() - 1), isBoolean));

    return sb.toString();
  }
}
