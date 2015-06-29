/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.chart;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Output ports which use this type automatically record the tuples output on them so
 * that they can later be used for charting (or for debugging) purpose.
 *
 * @param <T> type of the tuple emitted on this port
 * @since 0.3.2
 */
public class ChartingOutputPort<T> extends DefaultOutputPort<T>
{
  @Override
  public void setup(PortContext context)
  {
    context.getAttributes().put(PortContext.AUTO_RECORD, true);
  }

}
