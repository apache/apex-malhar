/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions;

import com.datatorrent.api.InputOperator;

/**
 * @since 3.1.0
 */

public interface InputGenerator<T> extends InputOperator
{
  public OutputPort<T> getOutputPort();
}
