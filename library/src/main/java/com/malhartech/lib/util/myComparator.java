/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.Comparator;

/**
 *
 * A comparator for ascending and descending lists<p>
 * <br>
 *
 * @author amol<br>
 *
 */
class myComparator<E> implements Comparator<E>
{
  public myComparator(boolean flag)
  {
    ascending = flag;
  }
  public boolean ascending = true;

  @Override
  public int compare(E e1, E e2)
  {
    Comparable<? super E> ce1 = (Comparable<? super E>)e1;
    int ret = ce1.compareTo(e2);
    if (!ascending) {
      ret = 0 - ret;
    }
    return ret;
  }
}
