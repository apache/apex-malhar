/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.math;

import com.datatorrent.api.DefaultInputPort;

/**
 * Transforms the input into the output after applying appropriate mathematical function to it and emits result on respective ports.
 * <p>
 * Emits the result as Long on port "longResult", as Integer on port "integerResult",as Double on port "doubleResult", and as Float on port "floatResult".
 * This is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longResult</b>: emits Long<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * @displayName Single Variable Abstract Calculus
 * @category Math
 * @tags numeric, single variable
 * @param <T>
 * @since 0.3.2
 */
public abstract class SingleVariableAbstractCalculus extends AbstractOutput
{
  /**
   * Input number port.
   */
  public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
  {
    @Override
    public void process(Number tuple)
    {
      Double dResult = null;
      if (doubleResult.isConnected()) {
        doubleResult.emit(dResult = function(tuple.doubleValue()));
      }

      if (floatResult.isConnected()) {
        floatResult.emit(dResult == null ? (float)function(tuple.doubleValue()) : dResult.floatValue());
      }

      Long lResult = null;
      if (longResult.isConnected()) {
        longResult.emit(lResult = function(tuple.longValue()));
      }

      if (integerResult.isConnected()) {
        integerResult.emit(lResult == null ? (int)function(tuple.longValue()) : lResult.intValue());
      }
    }

  };

  /**
   * Transform the input into the output after applying appropriate mathematical function to it.
   *
   * @param val
   * @return result of the function (double)
   */
  public abstract double function(double val);

  /**
   * Transform the input into the output after applying appropriate mathematical function to it.
   * @param val
   * @return result of the function (long)
   */
  public abstract long function(long val);

}
