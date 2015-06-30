/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.schemas;

import java.io.Serializable;
import java.text.DecimalFormat;

/**
 * <p>
 * The {@link ResultFormatter} applies user specified {@link DecimalFormat} rules
 * to data. This class is used when serializing query results in order to define
 * out output data should be formatted before being sent back to the user.
 * </p>
 * <p>
 * Currently the ResultFormatter does not support applying special formatting to individual fields,
 * it can only support broad formatting rules based on field type. For example all float fields will be
 * formatted a certain way and all float fields could potentially be formatted in another way. In the
 * future the result formatter will support formatting data uniquely for each individual field.
 * </p>
 */
public class ResultFormatter implements Serializable
{
  private static final long serialVersionUID = 201505121109L;

  /**
   * The {@link DecimalFormat} string for float data.
   */
  private String floatFormatString;
  /**
   * The {@link DecimalFormat} string for double data.
   */
  private String doubleFormatString;
  /**
   * The {@link DecimalFormat} string for byte data.
   */
  private String byteFormatString;
  /**
   * The {@link DecimalFomrat} string for short data.
   */
  private String shortFormatString;
  /**
   * The {@link DecimalFormat} string for int data.
   */
  private String intFormatString;
  /**
   * The {@link DecimalFormat} string for long data.
   */
  private String longFormatString;
  /**
   * The {@link DecimalFormat} string for discrete data.
   */
  private String discreteFormatString;
  /**
   * The {@link DecimalFormat} string for continuous data.
   */
  private String continuousFormatString;

  /**
   * The {@link DecimalFormat} object for floats.
   */
  private transient DecimalFormat floatFormat;
  /**
   * The {@link DecimalFormat} object for doubles.
   */
  private transient DecimalFormat doubleFormat;
  /**
   * The {@link DecimalFormat} object for bytes.
   */
  private transient DecimalFormat byteFormat;
  /**
   * The {@link DecimalFormat} object for shorts.
   */
  private transient DecimalFormat shortFormat;
  /**
   * The {@link DecimalFormat} object for ints.
   */
  private transient DecimalFormat intFormat;
  /**
   * The {@link DecimalFormat} object for longs.
   */
  private transient DecimalFormat longFormat;

  /**
   * Constructor for class.
   */
  public ResultFormatter()
  {
    //Nothing needs to be done here
  }

  /**
   * Applies the correct formatting to the given object.
   * @param object The object to apply formatting to and convert to a String.
   * @return The formatted object string.
   */
  public String format(Object object)
  {
    Type type = Type.CLASS_TO_TYPE.get(object.getClass());

    if(type == null) {
      return object.toString();
    }

    switch(type) {
      case FLOAT:
      {
        return format((float) ((Float) object));
      }
      case DOUBLE:
      {
        return format((double) ((Double) object));
      }
      case BYTE:
      {
        return format((byte) ((Byte) object));
      }
      case SHORT:
      {
        return format((short) ((Short) object));
      }
      case INTEGER:
      {
        return format((int) ((Integer) object));
      }
      case LONG:
      {
        return format((long) ((Long) object));
      }
      default:
        return object.toString();
    }
  }

  /**
   * Formats the given float value.
   * @param val The float value to format.
   * @return The formatted float value.
   */
  public String format(float val)
  {
    DecimalFormat df = getFloatFormat();

    if(df != null) {
      return df.format(val);
    }

    return Float.toString(val);
  }

  /**
   * Formats the given double values.
   * @param val The double value to format.
   * @return The formatted double value.
   */
  public String format(double val)
  {
    DecimalFormat df = getDoubleFormat();

    if(df != null) {
      return df.format(val);
    }

    return Double.toString(val);
  }

  /**
   * Formats the given byte value.
   * @param val The byte value to format.
   * @return The formatted byte value.
   */
  public String format(byte val)
  {
    DecimalFormat df = getByteFormat();

    if(df != null) {
      return df.format(val);
    }

    return Byte.toString(val);
  }

  /**
   * Formats the given short value.
   * @param val The short value to format.
   * @return The formatted short value.
   */
  public String format(short val)
  {
    DecimalFormat df = getShortFormat();

    if(df != null) {
      return df.format(val);
    }

    return Short.toString(val);
  }

  /**
   * Formats the given int value.
   * @param val The int value to format.
   * @return The formatted int value.
   */
  public String format(int val)
  {
    DecimalFormat df = getIntFormat();

    if(df != null) {
      return df.format(val);
    }

    return Integer.toString(val);
  }

  /**
   * Formats the given long value.
   * @param val The long value to format.
   * @return The formatted long value.
   */
  public String format(long val)
  {
    DecimalFormat df = getLongFormat();

    if(df != null) {
      return df.format(val);
    }

    return Long.toString(val);
  }

  /**
   * Gets the {@link DecimalFormat} object to use for floats.
   * @return The {@link DecimalFormat} object to use for floats.
   */
  public DecimalFormat getFloatFormat()
  {
    if(floatFormat == null && floatFormatString != null) {
      floatFormat = new DecimalFormat(floatFormatString);
    }

    return floatFormat;
  }

  /**
   * Gets the {@link DecimalFormat} for doubles.
   * @return The {@link DecimalFormat} object to use for doubles.
   */
  public DecimalFormat getDoubleFormat()
  {
    if(doubleFormat == null && doubleFormatString != null) {
      doubleFormat = new DecimalFormat(doubleFormatString);
    }

    return doubleFormat;
  }

  /**
   * Gets the {@link DecimalFormat} for bytes.
   * @return The {@link DecimalFormat} object to use for bytes.
   */
  public DecimalFormat getByteFormat()
  {
    if(byteFormat == null && byteFormatString != null) {
      byteFormat = new DecimalFormat(byteFormatString);
    }

    return byteFormat;
  }

  /**
   * Gets the {@link DecimalFormat} for shorts.
   * @return The {@link DecimalFormat} object to use for shorts.
   */
  public DecimalFormat getShortFormat()
  {
    if(shortFormat == null && shortFormatString != null) {
      shortFormat = new DecimalFormat(shortFormatString);
    }

    return shortFormat;
  }

  /**
   * Gets the {@link DecimalFormat} for ints.
   * @return The {@link DecimalFormat} object to use for ints.
   */
  public DecimalFormat getIntFormat()
  {
    if(intFormat == null && intFormatString != null) {
      intFormat = new DecimalFormat(intFormatString);
    }

    return intFormat;
  }

  /**
   * Gets the {@link DecimalFormat} for longs.
   * @return The {@link DecimalFormat} object to use for longs.
   */
  public DecimalFormat getLongFormat()
  {
    if(longFormat == null && longFormatString != null) {
      longFormat = new DecimalFormat(longFormatString);
    }

    return longFormat;
  }

  /**
   * Gets the {@link DecimalFormat} for discrete values.
   * @return The {@link DecimalFormat} object to use for discrete values.
   */
  public String getDiscreteFormatString()
  {
    return discreteFormatString;
  }

  /**
   * Sets the format string for discrete values.
   * @param discreteFormatString The format string for discrete values.
   */
  public void setDiscreteFormatString(String discreteFormatString)
  {
    this.discreteFormatString = discreteFormatString;
    this.byteFormatString = discreteFormatString;
    this.shortFormatString = discreteFormatString;
    this.intFormatString = discreteFormatString;
    this.longFormatString = discreteFormatString;
  }

  /**
   * Gets the format string for continuous values.
   * @return The format string for continuous values.
   */
  public String getContinuousFormatString()
  {
    return continuousFormatString;
  }

  /**
   * Sets the format string for continuous values.
   * @param continuousFormatString The format string for continuous values.
   */
  public void setContinuousFormatString(String continuousFormatString)
  {
    this.continuousFormatString = continuousFormatString;
    this.floatFormatString = continuousFormatString;
    this.doubleFormatString = continuousFormatString;
  }

  /**
   * Gets the float format string.
   * @return The float format string.
   */
  public String getFloatFormatString()
  {
    return floatFormatString;
  }

  /**
   * Sets the float format string.
   * @param decimalFormatString The float format string.
   */
  public void setFloatFormatString(String decimalFormatString)
  {
    this.floatFormatString = decimalFormatString;
  }

  /**
   * Gets the double format string.
   * @return The double format string.
   */
  public String getDoubleFormatString()
  {
    return doubleFormatString;
  }

  /**
   * Sets the double format string.
   * @param doubleFormatString The double format string.
   */
  public void setDoubleFormatString(String doubleFormatString)
  {
    this.doubleFormatString = doubleFormatString;
  }

  /**
   * Gets the int format string.
   * @return The int format string.
   */
  public String getIntFormatString()
  {
    return intFormatString;
  }

  /**
   * Sets the int format string.
   * @param intFormatString The int format string.
   */
  public void setIntFormatString(String intFormatString)
  {
    this.intFormatString = intFormatString;
  }

  /**
   * Gets the byte format string.
   * @return The byte format string.
   */
  public String getByteFormatString()
  {
    return byteFormatString;
  }

  /**
   * Sets the byte format string.
   * @param byteFormatString The byte format string to set.
   */
  public void setByteFormatString(String byteFormatString)
  {
    this.byteFormatString = byteFormatString;
  }

  /**
   * Gets the short format string.
   * @return The short format string.
   */
  public String getShortFormatString()
  {
    return shortFormatString;
  }

  /**
   * Sets the short format string.
   * @param shortFormatString The shortFormatString to set.
   */
  public void setShortFormatString(String shortFormatString)
  {
    this.shortFormatString = shortFormatString;
  }

  /**
   * Gets the long format string.
   * @return The long format string to set.
   */
  public String getLongFormatString()
  {
    return longFormatString;
  }

  /**
   * Sets the long format string.
   * @param longFormatString The long format string to set.
   */
  public void setLongFormatString(String longFormatString)
  {
    this.longFormatString = longFormatString;
  }
}
