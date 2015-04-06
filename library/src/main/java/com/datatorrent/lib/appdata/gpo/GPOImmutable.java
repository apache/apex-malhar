/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.schemas.Fields;
import java.io.Serializable;

public class GPOImmutable extends GPOMutable implements Serializable
{
  private static final long serialVersionUID = 201503231206L;

  private boolean constructorDone = false;

  public GPOImmutable()
  {
    //For kryo
    constructorDone = true;
  }

  public GPOImmutable(GPOMutable gpo)
  {
    super(gpo);
    constructorDone = true;
  }

  public GPOImmutable(GPOMutable gpo,
                      Fields subsetFields)
  {
    super(gpo,
          subsetFields);
    constructorDone = true;
  }

  @Override
  public boolean[] getFieldsBoolean()
  {
    throw new UnsupportedOperationException("Cannot get the boolean array.");
  }

  @Override
  public char[] getFieldsCharacter()
  {
    throw new UnsupportedOperationException("Cannot get the character array.");
  }

  @Override
  public byte[] getFieldsByte()
  {
    throw new UnsupportedOperationException("Cannot get the byte array.");
  }

  @Override
  public short[] getFieldsShort()
  {
    throw new UnsupportedOperationException("Cannot get the short array.");
  }

  @Override
  public int[] getFieldsInteger()
  {
    throw new UnsupportedOperationException("Cannot get the integer array.");
  }

  @Override
  public long[] getFieldsLong()
  {
    throw new UnsupportedOperationException("Cannot get the long array.");
  }

  @Override
  public float[] getFieldsFloat()
  {
    throw new UnsupportedOperationException("Cannot get the float array.");
  }

  @Override
  public double[] getFieldsDouble()
  {
    throw new UnsupportedOperationException("Cannot get the double array.");
  }

  @Override
  public void setField(String field, boolean val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, char val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, byte val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, short val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, int val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, long val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, float val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, double val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  private void throwImmutable()
  {
    throw new UnsupportedOperationException("Immutable");
  }
}
