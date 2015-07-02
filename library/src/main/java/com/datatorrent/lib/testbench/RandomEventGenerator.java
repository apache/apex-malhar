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
package com.datatorrent.lib.testbench;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.Random;
import javax.validation.constraints.Min;

/**
 * Generates synthetic load.&nbsp;Creates tuples using random numbers and keeps emitting them on the output port string_data and integer_data.
 * <p>
 * <br>
 * The load is generated as per config parameters. This class is mainly meant for testing nodes by creating a random number within
 * a range at a very high throughput. This node does not need to be windowed. It would just create tuple stream upto the limit set
 * by the config parameters.<br>
 * <br>
 * <b>Tuple Schema</b>: Has two choices Integer, or String<br><br>
 * <b>Port Interface</b>
 * <b>string_data</b>: Emits String tuples<br>
 * <b>integer_data</b>: Emits Integer tuples<br>
 * <b>Properties</b>:
 * <b>key</b> is an optional parameter, the generator sends an HashMap if key is specified<br>
 * <b>min_value</b> is the minimum value of the range of numbers. Default is 0<br>
 * <b>max_value</b> is the maximum value of the range of numbers. Default is 100<br>
 * <b>tuples_burst</b> is the total amount of tuples sent by the node before handing over control. The default
 * value is 10000. A high value does not help as if window has space the control is immediately returned for mode processing<br>
 * <b>string_schema</b> controls the tuple schema. For string set it to "true". By default it is "false" (i.e. Integer schema)<br>
 * <br>
 * Compile time checks are:<br>
 * <b>min_value</b> has to be an integer<br>
 * <b>max_value</b> has to be an integer and has to be >= min_value<br>
 * <b>tuples_burst</b>If specified must be an integer<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * This node has been benchmarked at over 10 million tuples/second in local/inline mode<br>
 * <br>
 * Compile time error checking includes<br>
 * </p>
 * @displayName Random Event Generator
 * @category Testbench
 * @tags generate
 * @since 0.3.2
 */
public class RandomEventGenerator extends BaseOperator implements InputOperator
{
  /**
   * The output port on which randomly generated integers are emitted as strings.
   */
  public final transient DefaultOutputPort<String> string_data = new DefaultOutputPort<String>();
  /**
   * The output port on which randomly generated integers are emitted.
   */
  public final transient DefaultOutputPort<Integer> integer_data = new DefaultOutputPort<Integer>();
  private int maxCountOfWindows = Integer.MAX_VALUE;
  @Min(1)
  private int tuplesBlast = 1000;
  @Min(1)
  private int tuplesBlastIntervalMillis = 10;
  private int min_value = 0;
  private int max_value = 100;
  private final Random random = new Random();

  public int getMaxvalue()
  {
    return max_value;
  }

  public int getMinvalue()
  {
    return min_value;
  }

  @Min(1)
  public int getTuplesBlast()
  {
    return tuplesBlast;
  }

  @Min(1)
  public int getTuplesBlastIntervalMillis()
  {
    return this.tuplesBlastIntervalMillis;
  }

  public void setMaxvalue(int i)
  {
    max_value = i;
  }

  public void setMinvalue(int i)
  {
    min_value = i;
  }

  public void setTuplesBlast(int i)
  {
    tuplesBlast = i;
  }

  public void setTuplesBlastIntervalMillis(int tuplesBlastIntervalMillis) {
    this.tuplesBlastIntervalMillis = tuplesBlastIntervalMillis;
  }

  @Override
  public void setup(OperatorContext context)
  {
    if (max_value <= min_value) {
      throw new IllegalArgumentException(String.format("min_value (%d) should be < max_value(%d)", min_value, max_value));
    }
  }

  /**
   * Maximum number of Windows across which this operator will work. 
   * @param i
  */
  public void setMaxCountOfWindows(int i)
  {
    maxCountOfWindows = i;
  }

  @Override
  public void endWindow()
  {
    if (--maxCountOfWindows == 0) {
      //Thread.currentThread().interrupt();
      throw new RuntimeException(new InterruptedException("Finished generating data."));
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    int range = max_value - min_value + 1;
    int i = 0;
    while (i < tuplesBlast) {
      int rval = min_value + random.nextInt(range);
      if (integer_data.isConnected()) {
        integer_data.emit(rval);
      }
      if (string_data.isConnected()) {
        string_data.emit(Integer.toString(rval));
      }
      i++;
    }

    if (tuplesBlastIntervalMillis > 0) {
      try {
        Thread.sleep(tuplesBlastIntervalMillis);
      } catch (InterruptedException e) {
      }
    }
  }
}
