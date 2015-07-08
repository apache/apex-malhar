/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions;

import com.datatorrent.api.InputOperator;

public interface InputGenerator<T> extends InputOperator
{
  public OutputPort<T> getOutputPort();
}
