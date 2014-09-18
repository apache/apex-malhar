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
package com.datatorrent.lib.stream;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;

/**
 * Merges two streams with identical schema and emits the tuples to the output port in order. 
 * 
 * <p>
 * This is a pass through operator<br>
 * <b>Ports</b>:<br>
 * <b>data1</b>: expects K<br>
 * <b>data2</b>: expects K<br>
 * <b>out</b>: emits K<br>
 * <br>
 * @displayName: Stream Merger
 * @category: stream
 * @tag: merge
 * @since 0.3.3
 */
@Stateless
public class StreamMerger<K> extends BaseOperator
{
	/**
	 * Data port 1.
	 */
  @InputPortFieldAnnotation(name = "data1")
 public final transient DefaultInputPort<K> data1 = new DefaultInputPort<K>()
  {
    /**
     * Emits to port "out"
     */
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  /**
   * Data port 2.
   */
  @InputPortFieldAnnotation(name = "data2")
  public final transient DefaultInputPort<K> data2 = new DefaultInputPort<K>()
  {
    /**
     * Emits to port "out"
     */
    @Override
    public void process(K tuple)
    {
      out.emit(tuple);
    }
  };

  /**
   * Output port.
   */
  @OutputPortFieldAnnotation(name = "out")
  public final transient DefaultOutputPort<K> out = new DefaultOutputPort<K>();
}
