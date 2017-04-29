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
package org.apache.apex.examples.twitter;

import java.io.Serializable;

/**
 * Developed for a example<br>
 *
 * @param <T> Type of object for which sliding window is being maintained.
 * @since 0.3.2
 */
public class SlidingContainer<T> implements Serializable
{
  private static final long serialVersionUID = 201305291751L;
  T identifier;
  int totalCount;
  int position;
  int[] windowedCount;

  @SuppressWarnings("unused")
  private SlidingContainer()
  {
    /* needed for Kryo serialization */
  }

  public SlidingContainer(T identifier, int windowCount)
  {
    this.identifier = identifier;
    this.totalCount = 0;
    this.position = 0;
    windowedCount = new int[windowCount];
  }

  public void adjustCount(int i)
  {
    windowedCount[position] += i;
  }

  public void slide()
  {
    int currentCount = windowedCount[position];
    position = position == windowedCount.length - 1 ? 0 : position + 1;
    totalCount += currentCount - windowedCount[position];
    windowedCount[position] = 0;
  }

  @Override
  public String toString()
  {
    return identifier + " => " + totalCount;
  }

}
