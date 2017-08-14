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
package org.apache.apex.malhar.lib.util;

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
    public long longVal = 15L;
    public float floatVal = 5.5f;
    public double doubleVal = 6.3;
    public Object objVal = Lists.newArrayList("bananas");

    protected boolean protectedBoolVal = boolVal;
    protected byte protectedByteVal = byteVal;
    protected char protectedCharVal = charVal;
    protected String protectedStringVal = stringVal;
    protected short protectedShortVal = shortVal;
    protected int protectedIntVal = intVal;
    protected long protectedLongVal = longVal;
    protected float protectedFloatVal = floatVal;
    protected double protectedDoubleVal = doubleVal;
    protected Object protectedObjVal = objVal;

    private boolean privateBoolVal = boolVal;
    private byte privateByteVal = byteVal;
    private char privateCharVal = charVal;
    private String privateStringVal = stringVal;
    private short privateShortVal = shortVal;
    private int privateIntVal = intVal;
    private long privateLongVal = longVal;
    private float privateFloatVal = floatVal;
    private double privateDoubleVal = doubleVal;
    private Object privateObjVal = objVal;

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

    public void setProtectedBoolVal(boolean protectedBoolVal)
    {
      this.protectedBoolVal = protectedBoolVal;
    }

    public void setProtectedByteVal(byte protectedByteVal)
    {
      this.protectedByteVal = protectedByteVal;
    }

    public void setProtectedCharVal(char protectedCharVal)
    {
      this.protectedCharVal = protectedCharVal;
    }

    public void setProtectedStringVal(String protectedStringVal)
    {
      this.protectedStringVal = protectedStringVal;
    }

    public void setProtectedShortVal(short protectedShortVal)
    {
      this.protectedShortVal = protectedShortVal;
    }

    public void setProtectedIntVal(int protectedIntVal)
    {
      this.protectedIntVal = protectedIntVal;
    }

    public void setProtectedLongVal(long protectedLongVal)
    {
      this.protectedLongVal = protectedLongVal;
    }

    public void setProtectedFloatVal(float protectedFloatVal)
    {
      this.protectedFloatVal = protectedFloatVal;
    }

    public void setProtectedDoubleVal(double protectedDoubleVal)
    {
      this.protectedDoubleVal = protectedDoubleVal;
    }

    public void setProtectedObjVal(Object protectedObjVal)
    {
      this.protectedObjVal = protectedObjVal;
    }

    public boolean isPrivateBoolVal()
    {
      return privateBoolVal;
    }

    public void setPrivateBoolVal(boolean privateBoolVal)
    {
      this.privateBoolVal = privateBoolVal;
    }

    public byte getPrivateByteVal()
    {
      return privateByteVal;
    }

    public void setPrivateByteVal(byte privateByteVal)
    {
      this.privateByteVal = privateByteVal;
    }

    public char getPrivateCharVal()
    {
      return privateCharVal;
    }

    public void setPrivateCharVal(char privateCharVal)
    {
      this.privateCharVal = privateCharVal;
    }

    public String getPrivateStringVal()
    {
      return privateStringVal;
    }

    public void setPrivateStringVal(String privateStringVal)
    {
      this.privateStringVal = privateStringVal;
    }

    public short getPrivateShortVal()
    {
      return privateShortVal;
    }

    public void setPrivateShortVal(short privateShortVal)
    {
      this.privateShortVal = privateShortVal;
    }

    public int getPrivateIntVal()
    {
      return privateIntVal;
    }

    public void setPrivateIntVal(int privateIntVal)
    {
      this.privateIntVal = privateIntVal;
    }

    public long getPrivateLongVal()
    {
      return privateLongVal;
    }

    public void setPrivateLongVal(long privateLongVal)
    {
      this.privateLongVal = privateLongVal;
    }

    public float getPrivateFloatVal()
    {
      return privateFloatVal;
    }

    public void setPrivateFloatVal(float privateFloatVal)
    {
      this.privateFloatVal = privateFloatVal;
    }

    public double getPrivateDoubleVal()
    {
      return privateDoubleVal;
    }

    public void setPrivateDoubleVal(double privateDoubleVal)
    {
      this.privateDoubleVal = privateDoubleVal;
    }

    public Object getPrivateObjVal()
    {
      return privateObjVal;
    }

    public void setPrivateObjVal(Object privateObjVal)
    {
      this.privateObjVal = privateObjVal;
    }

  }
}
