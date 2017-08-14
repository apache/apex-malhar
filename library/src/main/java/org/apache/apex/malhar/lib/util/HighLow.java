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
package org.apache.apex.malhar.lib.util;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model. <p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @since 0.3.2
 */
public class HighLow<V extends Number>
{
  protected V high;
  protected V low;

  /**
   * Added default constructor for deserializer.
   */
  public HighLow()
  {
    high = null;
    low = null;
  }

  /**
   * Constructor
   *
   * @param h
   * @param l
   */
  public HighLow(V h, V l)
  {
    high = h;
    low = l;
  }

  /**
   * @return high value
   */
  public V getHigh()
  {
    return high;
  }

  /**
   *
   * @return low value
   */
  public V getLow()
  {
    return low;
  }

  /**
   * @param h sets high value
   */
  public void setHigh(V h)
  {
    high = h;
  }

  /**
   *
   * @param l sets low value
   */
  public void setLow(V l)
  {
    low = l;
  }

  @Override
  public String toString()
  {
    return "(" + low.toString() + "," + high.toString() + ")";
  }

}
