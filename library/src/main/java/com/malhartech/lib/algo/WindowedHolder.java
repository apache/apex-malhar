/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import java.util.Comparator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
class WindowedHolder<T>
{
  T identifier;
  int totalCount;
  int position;
  int windowedCount[];

  WindowedHolder()
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
