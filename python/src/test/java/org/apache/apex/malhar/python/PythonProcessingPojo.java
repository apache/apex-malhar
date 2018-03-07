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

import org.apache.apex.malhar.python.base.util.NDimensionalArray;

public class PythonProcessingPojo
{

  private float y;

  private int x;


  private NDimensionalArray<float[]> numpyFloatArray;

  private NDimensionalArray<int[]> numpyIntArray;


  public float getY()
  {
    return y;
  }

  public void setY(float y)
  {
    this.y = y;
  }

  public int getX()
  {
    return x;
  }

  public void setX(int x)
  {
    this.x = x;
  }

  public NDimensionalArray<float[]> getNumpyFloatArray()
  {
    return numpyFloatArray;
  }

  public void setNumpyFloatArray(NDimensionalArray<float[]> numpyFloatArray)
  {
    this.numpyFloatArray = numpyFloatArray;
  }

  public NDimensionalArray<int[]> getNumpyIntArray()
  {
    return numpyIntArray;
  }

  public void setNumpyIntArray(NDimensionalArray<int[]> numpyIntArray)
  {
    this.numpyIntArray = numpyIntArray;
  }
}
