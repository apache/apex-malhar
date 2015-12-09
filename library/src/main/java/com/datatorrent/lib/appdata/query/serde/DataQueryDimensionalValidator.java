/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.query.serde;

import com.datatorrent.lib.appdata.schemas.Message;

/**
 * This is the validator for {@link com.datatorrent.lib.appdata.schemas.DataQueryDimensional} objects.
 *
 * @since 3.1.0
 */
public class DataQueryDimensionalValidator implements CustomMessageValidator
{
  /**
   * Constructor used to instantiate validator in {@link MessageDeserializerFactory}.
   */
  public DataQueryDimensionalValidator()
  {
  }

  @Override
  public boolean validate(Message query, Object context)
  {
    return true;
  }
}
