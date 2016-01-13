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

public class ConversionUtils
{
  public static int toInt(double a)
  {
    return new Double(a).intValue();
  }

  public static int toInt(float a)
  {
    return new Float(a).intValue();
  }

  public static int toInt(long a)
  {
    return new Long(a).intValue();
  }

  public static int toInt(byte a)
  {
    return new Byte(a).intValue();
  }

  public static int toInt(short a)
  {
    return new Short(a).intValue();
  }

  public static int toInt(String a)
  {
    return NumberUtils.toInt(a);
  }

  public static long toLong(float a)
  {
    return new Float(a).longValue();
  }

  public static long toLong(double a)
  {
    return new Double(a).longValue();
  }

  public static long toLong(int a)
  {
    return new Integer(a).longValue();
  }

  public static long toLong(byte a)
  {
    return new Byte(a).longValue();
  }

  public static long toLong(short a)
  {
    return new Short(a).longValue();
  }

  public static long toLong(String a)
  {
    return NumberUtils.toLong(a);
  }

  public static int toShort(double a)
  {
    return new Double(a).shortValue();
  }

  public static short toShort(float a)
  {
    return new Float(a).shortValue();
  }

  public static short toShort(long a)
  {
    return new Long(a).shortValue();
  }

  public static short toShort(byte a)
  {
    return new Byte(a).shortValue();
  }

  public static short toShort(int a)
  {
    return new Integer(a).shortValue();
  }

  public static short toShort(String a)
  {
    return NumberUtils.toShort(a);
  }

  public static byte toByte(float a)
  {
    return new Float(a).byteValue();
  }

  public static byte toByte(double a)
  {
    return new Double(a).byteValue();
  }

  public static byte toByte(int a)
  {
    return new Integer(a).byteValue();
  }

  public static byte toByte(long a)
  {
    return new Long(a).byteValue();
  }

  public static byte toByte(short a)
  {
    return new Short(a).byteValue();
  }

  public static byte toByte(String a)
  {
    return NumberUtils.toByte(a);
  }

  public static float toFloat(double a)
  {
    return new Double(a).floatValue();
  }

  public static float toFloat(short a)
  {
    return new Short(a).floatValue();
  }

  public static float toFloat(long a)
  {
    return new Long(a).floatValue();
  }

  public static float toFloat(byte a)
  {
    return new Byte(a).floatValue();
  }

  public static float toFloat(int a)
  {
    return new Integer(a).floatValue();
  }

  public static float toFloat(String a)
  {
    return NumberUtils.toFloat(a);
  }

  public static double toDouble(float a)
  {
    return new Float(a).doubleValue();
  }

  public static double toDouble(byte a)
  {
    return new Byte(a).doubleValue();
  }

  public static double toDouble(int a)
  {
    return new Integer(a).doubleValue();
  }

  public static double toDouble(long a)
  {
    return new Long(a).doubleValue();
  }

  public static double toDouble(short a)
  {
    return new Short(a).doubleValue();
  }

  public static double toDouble(String a)
  {
    return NumberUtils.toDouble(a);
  }
}
