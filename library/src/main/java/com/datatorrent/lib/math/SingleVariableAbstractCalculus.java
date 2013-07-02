/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.math;

import com.datatorrent.api.DefaultInputPort;

/**
 *
 * Abstract class that output of function(input_tuple). Emits the result as Long on port \"longResult\", as Integer on port \"integerResult\",
 * as Double on port \"doubleResult\", and as Float on port \"floatResult\". This is a pass through operator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longResult</b>: emits Long<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <p>
 * <b>Abstract Functions:</b><br>
 * <b>function(double)</b>: For output ports doubleResult, and floatResult<br>
 * <b>function(long)</b>: For output ports longResult, and integerResult<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode for function(x) = x*x<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for SingleVariableAbstractCalculus">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>25 million tuples/s</b></td><td>four tuples per one incoming tuple</td><td>Performance is i/o bound and directly
 * dependant on incoming tuple rate</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table for abstract function of f(x) = x * x</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for SingleVariableAbstractCalculus">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (<i>input</i>::process)</th><th colspan=4>Out-bound (emit)</th></tr>
 * <tr><th><i>input</i></th><th><i>longResult</i></th><th><i>integerResult</i></th><th><i>doubleResult</i></th><th><i>floatResult</i></th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>2</td><td>4</td><td>4</td><td>4.0</td><td>4.0</td></tr>
 * <tr><td>Data (process())</td><td>-12</td><td>144</td><td>144</td><td>144.0</td><td>144.0</td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 *
 */
public abstract class SingleVariableAbstractCalculus extends AbstractOutports
{
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
