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
package com.datatorrent.demos.rollingtopwords;

import java.io.Serializable;
import java.util.Comparator;


/**
 * Developed for a demo<br>
 *
 * @since 0.3.2
 */
public class WindowedHolder<T> implements Serializable
{
  private static final long serialVersionUID = 201305291751L;
  T identifier;
  int totalCount;
  int position;
  int windowedCount[];

  @SuppressWarnings("unused")
  private WindowedHolder()
  {
  }

  public WindowedHolder(T identifier, int windowCount)
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

class TopSpotComparator implements Comparator<WindowedHolder<?>>
{
  @Override
  public int compare(WindowedHolder<?> o1, WindowedHolder<?> o2)
  {
    if (o1.totalCount > o2.totalCount) {
      return 1;
    }
    else if (o1.totalCount < o2.totalCount) {
      return -1;
    }

    return 0;
  }
}
