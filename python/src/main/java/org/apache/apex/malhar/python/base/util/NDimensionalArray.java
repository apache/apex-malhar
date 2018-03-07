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
package org.apache.apex.malhar.python.base.util;

import jep.NDArray;

/***
 * Represents a wrapper around a numpy array. The way to build a numpy array is by first creating a single
 *  dimensional array and then specifying the dimensions. The dimensions specify how the single dimension will slice
 *   the single dimensional array.
 * @param <T> The data type of the single dimensional array. For example for a numpy array of type float, T will be
 *           of type float[].
 *           <p>
 *           Only the following types are supported:
 *          <ol>
 *           <li>float[]</li>
 *           <li>int[]</li>
 *           <li>double[]</li>
 *           <li>long[]</li>
 *           <li>short[]</li>
 *           <li>byte[]</li>
 *           <li>boolean[]</li>
 *          </ol>
 *           </p>
 *           <p>No support for complex types. See example application in test modules for code snippets & usage</p>
 */
public class NDimensionalArray<T>
{

  int[] dimensions;

  T data;

  int lengthOfSequentialArray;

  boolean signedFlag;

  public NDimensionalArray()
  {
  }

  public NDArray<T> toNDArray()
  {
    return new NDArray<T>(data,signedFlag,dimensions);
  }

  public int[] getDimensions()
  {
    return dimensions;
  }

  public void setDimensions(int[] dimensions)
  {
    this.dimensions = dimensions;
  }

  public int getLengthOfSequentialArray()
  {
    return lengthOfSequentialArray;
  }

  public void setLengthOfSequentialArray(int lengthOfSequentialArray)
  {
    this.lengthOfSequentialArray = lengthOfSequentialArray;
  }

  public boolean isSignedFlag()
  {
    return signedFlag;
  }

  public void setSignedFlag(boolean signedFlag)
  {
    this.signedFlag = signedFlag;
  }

  public T getData()
  {
    return data;
  }

  public void setData(T data)
  {
    this.data = data;
  }
}
