/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOImmutable extends GPOMutable
{
  public GPOImmutable(GPOMutable gpo)
  {
    super(gpo.getFieldToType());
  }

  @Override
  public void setField(String field, Boolean val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Character val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Byte val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Short val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Integer val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Long val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Float val)
  {
    this.throwImmutable();
  }

  @Override
  public void setField(String field, Double val)
  {
    this.throwImmutable();
  }

  private void throwImmutable()
  {
    throw new UnsupportedOperationException("Immutable");
  }
}
