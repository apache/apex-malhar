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
package com.datatorrent.lib.expressions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils
{
  public static boolean equalsWithCase(String str1, String str2)
  {
    return org.apache.commons.lang3.StringUtils.equals(str1, str2);
  }

  public static String truncate(String str, int length)
  {
    return (str == null) ? null : str.substring(0, length);
  }

  public static String regexCapture(String str, String regex, int groupId)
  {
    if (groupId <= 0) {
      return null;
    }

    Pattern entry = Pattern.compile(regex);
    Matcher matcher = entry.matcher(str);

    return matcher.find() ? matcher.group(groupId) : null;
  }
}
