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

/**
 * This class contains some public static method which are utility methods available in expression to be invoked.
 *
 * This set of methods takes care of some basic functionality associated with string operations.
 */
public class StringUtils
{
  /**
   * Checks if given 2 string are equals.
   * @param str1 First string for comparison
   * @param str2 Second string for comparision
   * @return Returns true if str1 & str2 are equals. Else false.
   */
  public static boolean equalsWithCase(String str1, String str2)
  {
    return org.apache.commons.lang3.StringUtils.equals(str1, str2);
  }

  /**
   * Truncate the string to given length.
   * @param str String to be truncated
   * @param length Length to which given string is to be truncated
   * @return Returns the truncated string
   */
  public static String truncate(String str, int length)
  {
    return (str == null) ? null : str.substring(0, length);
  }

  /**
   * Matches given regex for the string and returns the matched pattern indicated by groupId.
   * @param str String which need to be searched.
   * @param regex Regex pattern to be captures. Should contain groups.
   * @param groupId Index of matching pattern to be returned.
   *                0 returns complete string pattern match. >0 returns group for given index.
   * @return Returns matched group pattern for given index.
   */
  public static String regexCapture(String str, String regex, int groupId)
  {
    if (groupId <= 0) {
      return null;
    }

    Pattern entry = Pattern.compile(regex);
    Matcher matcher = entry.matcher(str);

    try {
      return matcher.find() ? matcher.group(groupId) : null;
    } catch (IllegalStateException | IndexOutOfBoundsException e) {
      return null;
    }
  }
}
