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
package com.datatorrent.lib.pigquery;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * A derivation of BaseOperator that implements Pig Split operator semantic.  <br>
 * <p>
 * Number of output ports are configured in class instance.
 *
 * <b>Properties : </b> <br>
 * <b>numOutPorts : </b> Number of output ports. <br>
 * @displayName: Pig Split
 * @category: pigquery
 * @tag: split operator, integer
 * @since 0.3.4
 */
public abstract class PigSplitOperator<T>  extends BaseOperator
{
  /**
   * Number of output ports.
   */
  private int numOutPorts = 1;
  
  /**
   * Constructor
   */
  public PigSplitOperator(int numOutPorts) {
    if (numOutPorts > 1) this.numOutPorts = numOutPorts;
    for (int i=0; i < numOutPorts; i++) {
      outports.add(new DefaultOutputPort<T>());
    }
  }
  
  /**
   * Input port.
   */
  public final transient DefaultInputPort<T> inport = new DefaultInputPort<T>() {
    @Override
    public void process(T tuple)
    {
      for (int i=0; i < numOutPorts; i++) {
        if (isValidEmit(i, tuple)) {
          outports.get(i).emit(tuple);
        }
      }
    }
   };

   /**
    * Abstract function to check if valid port for emitting tuple. 
    * @param i  Port number on which emit tuple. 
    * @param tuple  Tuple value to emit on port. 
    * @return emit status 
    */
  abstract public boolean isValidEmit(int i, T tuple);
  
  /**
   * Output port.
   */
  public transient List<DefaultOutputPort<T>> outports = new ArrayList<DefaultOutputPort<T>>();
}
