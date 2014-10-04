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

package com.datatorrent.benchmark;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import java.util.Random;
import javax.validation.constraints.Min;

/**
 * This is an input operator which generates random tuples that are an array of bytes.
 */
public class RandomWordGenerator implements InputOperator
{
  /**
   * The output port on which byte arrays are emitted.
   */
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  /**
   * The default number of tuples emitted per window.
   */
  public static final int MAX_TUPLES_PER_WINDOW = 200000;

  /**
   * The size of tuples in bytes.
   */
  public static final int TUPLE_BYTE_SIZE = 32;

  /**
   * The number of tuples per window.
   */
  @Min(1)
  private int tuplesPerWindow = MAX_TUPLES_PER_WINDOW;

  /**
   * The size of tuples in bytes.
   */
  @Min(1)
  private int tupleByteSize = TUPLE_BYTE_SIZE;

  /**
   * The random object use to generate the tuples.
   */
  private transient Random random = new Random();

  /**
   * A counter which is used to emit the same number of tuples per
   * application window.
   */
  private int tupleCounter = 0;

  public RandomWordGenerator()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    tupleCounter = 0;
  }

  @Override
  public void emitTuples()
  {
    for(;
        tupleCounter < tuplesPerWindow;
        tupleCounter++)
    {
      byte[] bytes = new byte[tupleByteSize];
      random.nextBytes(bytes);
      output.emit(bytes);
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   * Sets the number of tuples emitted per application window.
   * @param tuplesPerWindow The number of tuples emitted per application window.
   */
  public void setTuplesPerWindow(int tuplesPerWindow)
  {
    this.tuplesPerWindow = tuplesPerWindow;
  }

  /**
   * Gets the number of tuples emitted per application window.
   * @return The number of tuples emitted per application window.
   */
  public int getTuplesPerWindow()
  {
    return tuplesPerWindow;
  }

  /**
   * Sets the number of bytes in the emitted byte array tuples.
   * @param tupleByteSize The number of bytes in the emitted byte array tuples.
   */
  public void setTupleByteSize(int tupleByteSize)
  {
    this.tupleByteSize = tupleByteSize;
  }

  /**
   * Gets the number of bytes in the emitted byte array tuples.
   * @return The number of bytes in the emitted byte array tuples.
   */
  public int getTupleByteSize()
  {
    return tupleByteSize;
  }
}
