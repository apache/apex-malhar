/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import java.util.ArrayList;
import java.util.Collection;

/**
 *
 * @param <T> Type of elements in the collection.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ArrayListAggregator<T> extends AbstractAggregator<T>
{
  @Override
  public Collection<T> getNewCollection(int size)
  {
    return new ArrayList<T>(size);
  }

}
