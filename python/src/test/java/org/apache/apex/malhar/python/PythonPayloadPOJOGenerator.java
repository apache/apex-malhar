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

package org.apache.apex.malhar.python;

import java.util.Random;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.python.base.util.NDimensionalArray;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 * Generates and emits a simple float and int for python operator to consume
 *
 * @since 3.8.0
 */
public class PythonPayloadPOJOGenerator implements InputOperator
{
  private long tuplesCounter = 0;
  private long currentWindowTuplesCounter = 0;

  // Limit number of emitted tuples per window
  @Min(1)
  private long maxTuplesPerWindow = 150;

  @Min(1)
  private long maxTuples = 300;

  private final Random random = new Random();

  private static final int MAX_RANDOM_INT = 100;

  public static final int DIMENSION_SIZE = 2;

  public static int[] intDimensionSums = new int[ DIMENSION_SIZE * DIMENSION_SIZE ];

  public static float[] floatDimensionSums = new float[DIMENSION_SIZE * DIMENSION_SIZE];

  public final transient DefaultOutputPort<PythonProcessingPojo> output = new DefaultOutputPort<>();

  public PythonPayloadPOJOGenerator()
  {
    for ( int i = 0; i < (DIMENSION_SIZE * DIMENSION_SIZE ); i++) {
      intDimensionSums[i] = 0;
      floatDimensionSums[i] = 0;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowTuplesCounter = 0;
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void emitTuples()
  {
    while ( ( currentWindowTuplesCounter < maxTuplesPerWindow) && (tuplesCounter < maxTuples) ) {
      PythonProcessingPojo pythonProcessingPojo = new PythonProcessingPojo();
      pythonProcessingPojo.setX(random.nextInt(MAX_RANDOM_INT));
      pythonProcessingPojo.setY(random.nextFloat());

      float[] f = new float[( DIMENSION_SIZE * DIMENSION_SIZE)];
      for ( int i = 0; i < (DIMENSION_SIZE * DIMENSION_SIZE ); i++) {
        f[i] = random.nextFloat();
        floatDimensionSums[ i % DIMENSION_SIZE ] = floatDimensionSums[ i % DIMENSION_SIZE ] + f[i];
      }
      NDimensionalArray<float[]> nDimensionalFloatArray = new NDimensionalArray<>();
      nDimensionalFloatArray.setData(f);
      nDimensionalFloatArray.setDimensions(new int[] {DIMENSION_SIZE, DIMENSION_SIZE});
      nDimensionalFloatArray.setLengthOfSequentialArray(floatDimensionSums.length);
      nDimensionalFloatArray.setSignedFlag(false);
      pythonProcessingPojo.setNumpyFloatArray(nDimensionalFloatArray);

      int[] ints = new int[( DIMENSION_SIZE * DIMENSION_SIZE)];
      for ( int i = 0; i < (DIMENSION_SIZE * DIMENSION_SIZE ); i++) {
        ints[i] = random.nextInt(MAX_RANDOM_INT);
        intDimensionSums[ i % DIMENSION_SIZE ] = intDimensionSums [ i % DIMENSION_SIZE ] + ints[i];
      }
      NDimensionalArray<int[]> nDimensionalIntArray = new NDimensionalArray<>();
      nDimensionalIntArray.setData(ints);
      nDimensionalIntArray.setDimensions(new int[] {DIMENSION_SIZE, DIMENSION_SIZE});
      nDimensionalIntArray.setLengthOfSequentialArray(ints.length);
      nDimensionalIntArray.setSignedFlag(false);
      pythonProcessingPojo.setNumpyIntArray(nDimensionalIntArray);
      output.emit(pythonProcessingPojo);
      currentWindowTuplesCounter += 1;
      tuplesCounter += 1;
    }
  }

  public long getMaxTuples()
  {
    return maxTuples;
  }

  public void setMaxTuples(long maxTuples)
  {
    this.maxTuples = maxTuples;
  }

  public long getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }

  public static int[] getIntDimensionSums()
  {
    return intDimensionSums;
  }

  public static void setIntDimensionSums(int[] intDimensionSums)
  {
    PythonPayloadPOJOGenerator.intDimensionSums = intDimensionSums;
  }

  public static float[] getFloatDimensionSums()
  {
    return floatDimensionSums;
  }

  public static void setFloatDimensionSums(float[] floatDimensionSums)
  {
    PythonPayloadPOJOGenerator.floatDimensionSums = floatDimensionSums;
  }
}
