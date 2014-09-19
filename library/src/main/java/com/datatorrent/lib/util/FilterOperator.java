/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.util;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Filter operator intended for use with alerts
 *
 * @since 0.3.4
 */
public abstract class FilterOperator extends BaseOperator
{
  @InputPortFieldAnnotation(name = "in", optional = false)
  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object tuple)
    {
      if (satisfiesFilter(tuple)) {
        out.emit(tuple);
      }
    }

  };
  @OutputPortFieldAnnotation(name = "out", optional = false)
  public final transient DefaultOutputPort<Object> out = new DefaultOutputPort<Object>();

  public abstract boolean satisfiesFilter(Object tuple);
}
