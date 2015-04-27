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

import com.google.common.collect.Lists;

public class TestObjAllTypes
{
  public InnerObj innerObj = new InnerObj();

  /**
   * @return the innerObj
   */
  public InnerObj getInnerObj()
  {
    return innerObj;
  }

  /**
   * @param innerObj the innerObj to set
   */
  public void setInnerObj(InnerObj innerObj)
  {
    this.innerObj = innerObj;
  }

  public class InnerObj
  {
    public InnerObj()
    {
    }

    public boolean boolVal = true;
    public byte byteVal = 5;
    public char charVal = 'a';
    public String stringVal = "hello";
    public short shortVal = 10;
    public int intVal = 11;
    public long longVal = 15;
    public float floatVal = 5.5f;
    public double doubleVal = 6.3;
    public Object objVal = Lists.newArrayList("bananas");

    /**
     * @return the boolVal
     */
    public boolean isBoolVal()
    {
      return boolVal;
    }

    /**
     * @param boolVal the boolVal to set
     */
    public void setBoolVal(boolean boolVal)
    {
      this.boolVal = boolVal;
    }

    /**
     * @return the byteVal
     */
    public byte getByteVal()
    {
      return byteVal;
    }

    /**
     * @param byteVal the byteVal to set
     */
    public void setByteVal(byte byteVal)
    {
      this.byteVal = byteVal;
    }

    /**
     * @return the charVal
     */
    public char getCharVal()
    {
      return charVal;
    }

    /**
     * @param charVal the charVal to set
     */
    public void setCharVal(char charVal)
    {
      this.charVal = charVal;
    }

    /**
     * @return the stringVal
     */
    public String getStringVal()
    {
      return stringVal;
    }

    /**
     * @param stringVal the stringVal to set
     */
    public void setStringVal(String stringVal)
    {
      this.stringVal = stringVal;
    }

    /**
     * @return the shortVal
     */
    public short getShortVal()
    {
      return shortVal;
    }

    /**
     * @param shortVal the shortVal to set
     */
    public void setShortVal(short shortVal)
    {
      this.shortVal = shortVal;
    }

    /**
     * @return the intVal
     */
    public int getIntVal()
    {
      return intVal;
    }

    /**
     * @param intVal the intVal to set
     */
    public void setIntVal(int intVal)
    {
      this.intVal = intVal;
    }

    /**
     * @return the longVal
     */
    public long getLongVal()
    {
      return longVal;
    }

    /**
     * @param longVal the longVal to set
     */
    public void setLongVal(long longVal)
    {
      this.longVal = longVal;
    }

    /**
     * @return the floatVal
     */
    public float getFloatVal()
    {
      return floatVal;
    }

    /**
     * @param floatVal the floatVal to set
     */
    public void setFloatVal(float floatVal)
    {
      this.floatVal = floatVal;
    }

    /**
     * @return the doubleVal
     */
    public double getDoubleVal()
    {
      return doubleVal;
    }

    /**
     * @param doubleVal the doubleVal to set
     */
    public void setDoubleVal(double doubleVal)
    {
      this.doubleVal = doubleVal;
    }

    /**
     * @return the objVal
     */
    public Object getObjVal()
    {
      return objVal;
    }

    /**
     * @param objVal the objVal to set
     */
    public void setObjVal(Object objVal)
    {
      this.objVal = objVal;
    }
  }
}
