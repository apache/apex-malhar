/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.gpo;

import com.datatorrent.lib.appdata.schemas.Fields;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOImmutable extends GPOMutable
{
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
  public void setField(String field, Boolean val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Character val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Byte val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Short val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Integer val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Long val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Float val)
  {
    if(constructorDone) {
      this.throwImmutable();
    }

    super.setField(field, val);
  }

  @Override
  public void setField(String field, Double val)
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
