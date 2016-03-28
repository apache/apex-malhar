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

import org.apache.commons.lang3.math.NumberUtils;

/**
 * This class contains some public static method which are utility methods available in expression to be invoked.
 *
 * This set of methods takes care of conversion from one primitive type to another.
 */
public class ConversionUtils
{
  /**
   * Converts a double to int.
   */
  public static int toInt(double a)
  {
    return new Double(a).intValue();
  }

  /**
   * Converts a float to int.
   */
  public static int toInt(float a)
  {
    return new Float(a).intValue();
  }

  /**
   * Converts a long to int.
   */
  public static int toInt(long a)
  {
    return new Long(a).intValue();
  }

  /**
   * Converts a byte to int.
   */
  public static int toInt(byte a)
  {
    return new Byte(a).intValue();
  }

  /**
   * Converts a short to int.
   */
  public static int toInt(short a)
  {
    return new Short(a).intValue();
  }

  /**
   * Parses a String into an int.
   * It internally uses NumberUtils.toInt method for conversion.
   *
   * For empty string, returns 0.
   * For null string, returns 0.
   * If conversion fails, return 0.
   * For all other type, return int represented in the string.
   */
  public static int toInt(String a)
  {
    return NumberUtils.toInt(a);
  }

  /**
   * Converts a float to long.
   */
  public static long toLong(float a)
  {
    return new Float(a).longValue();
  }

  /**
   * Converts a double to long.
   */
  public static long toLong(double a)
  {
    return new Double(a).longValue();
  }

  /**
   * Converts a int to long.
   */
  public static long toLong(int a)
  {
    return new Integer(a).longValue();
  }

  /**
   * Converts a byte to long.
   */
  public static long toLong(byte a)
  {
    return new Byte(a).longValue();
  }

  /**
   * Converts a short to long.
   */
  public static long toLong(short a)
  {
    return new Short(a).longValue();
  }

  /**
   * Parses a String into a long.
   * It internally uses NumberUtils.toLong method for conversion.
   *
   * For empty string, returns 0.
   * For null string, returns 0.
   * If conversion fails, return 0.
   * For all other type, return int represented in the string.
   */
  public static long toLong(String a)
  {
    return NumberUtils.toLong(a);
  }

  /**
   * Converts a double to short.
   */
  public static int toShort(double a)
  {
    return new Double(a).shortValue();
  }

  /**
   * Converts a double to short.
   */
  public static short toShort(float a)
  {
    return new Float(a).shortValue();
  }

  /**
   * Converts a long to short.
   */
  public static short toShort(long a)
  {
    return new Long(a).shortValue();
  }

  /**
   * Converts a byte to short.
   */
  public static short toShort(byte a)
  {
    return new Byte(a).shortValue();
  }

  /**
   * Converts a int to short.
   */
  public static short toShort(int a)
  {
    return new Integer(a).shortValue();
  }

  /**
   * Parses a String into a short.
   * It internally uses NumberUtils.toShort method for conversion.
   *
   * For empty string, returns 0.
   * For null string, returns 0.
   * If conversion fails, return 0.
   * For all other type, return short represented in the string.
   */
  public static short toShort(String a)
  {
    return NumberUtils.toShort(a);
  }

  /**
   * Converts a float to byte.
   */
  public static byte toByte(float a)
  {
    return new Float(a).byteValue();
  }

  /**
   * Converts a double to byte.
   */
  public static byte toByte(double a)
  {
    return new Double(a).byteValue();
  }

  /**
   * Converts a int to byte.
   */
  public static byte toByte(int a)
  {
    return new Integer(a).byteValue();
  }

  /**
   * Converts a long to byte.
   */
  public static byte toByte(long a)
  {
    return new Long(a).byteValue();
  }

  /**
   * Converts a short to byte.
   */
  public static byte toByte(short a)
  {
    return new Short(a).byteValue();
  }

  /**
   * Parses a String into a byte.
   * It internally uses NumberUtils.toByte method for conversion.
   *
   * For empty string, returns 0.
   * For null string, returns 0.
   * If conversion fails, return 0.
   * For all other type, return byte represented in the string.
   */
  public static byte toByte(String a)
  {
    return NumberUtils.toByte(a);
  }

  /**
   * Converts a double to float.
   */
  public static float toFloat(double a)
  {
    return new Double(a).floatValue();
  }

  /**
   * Converts a short to float.
   */
  public static float toFloat(short a)
  {
    return new Short(a).floatValue();
  }

  /**
   * Converts a long to float.
   */
  public static float toFloat(long a)
  {
    return new Long(a).floatValue();
  }

  /**
   * Converts a byte to float.
   */
  public static float toFloat(byte a)
  {
    return new Byte(a).floatValue();
  }

  /**
   * Converts a int to float.
   */
  public static float toFloat(int a)
  {
    return new Integer(a).floatValue();
  }

  /**
   * Parses a String into a float.
   * It internally uses NumberUtils.toFloat method for conversion.
   *
   * For empty string, returns 0.
   * For null string, returns 0.
   * If conversion fails, return 0.
   * For all other type, return float represented in the string.
   */
  public static float toFloat(String a)
  {
    return NumberUtils.toFloat(a);
  }

  /**
   * Converts a float to double.
   */
  public static double toDouble(float a)
  {
    return new Float(a).doubleValue();
  }

  /**
   * Converts a byte to double.
   */
  public static double toDouble(byte a)
  {
    return new Byte(a).doubleValue();
  }

  /**
   * Converts a int to double.
   */
  public static double toDouble(int a)
  {
    return new Integer(a).doubleValue();
  }

  /**
   * Converts a long to double.
   */
  public static double toDouble(long a)
  {
    return new Long(a).doubleValue();
  }

  /**
   * Converts a short to double.
   */
  public static double toDouble(short a)
  {
    return new Short(a).doubleValue();
  }

  /**
   * Parses a String into a double.
   * It internally uses NumberUtils.toDouble method for conversion.
   *
   * For empty string, returns 0.
   * For null string, returns 0.
   * If conversion fails, return 0.
   * For all other type, return double represented in the string.
   */
  public static double toDouble(String a)
  {
    return NumberUtils.toDouble(a);
  }
}
