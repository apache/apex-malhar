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
package org.apache.apex.malhar.lib.utils.serde;


import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of {@link Serde} which serializes and deserializes pairs.
 */
@InterfaceStability.Evolving
public class SerdePairSlice<T1, T2> implements Serde<Pair<T1, T2>, Slice>
{
  @NotNull
  private Serde<T1, Slice> serde1;
  @NotNull
  private Serde<T2, Slice> serde2;

  private SerdePairSlice()
  {
    // for Kryo
  }

  /**
   * Creates a {@link SerdePairSlice}.
   * @param serde1 The {@link Serde} that is used to serialize and deserialize first element of a pair
   * @param serde2 The {@link Serde} that is used to serialize and deserialize second element of a pair
   */
  public SerdePairSlice(@NotNull Serde<T1, Slice> serde1, @NotNull Serde<T2, Slice> serde2)
  {
    this.serde1 = Preconditions.checkNotNull(serde1);
    this.serde2 = Preconditions.checkNotNull(serde2);
  }

  @Override
  public Slice serialize(Pair<T1, T2> pair)
  {
    int size = 0;

    Slice slice1 = serde1.serialize(pair.getLeft());
    size += slice1.length;
    Slice slice2 = serde2.serialize(pair.getRight());
    size += slice2.length;

    byte[] bytes = new byte[size];
    System.arraycopy(slice1.buffer, slice1.offset, bytes, 0, slice1.length);
    System.arraycopy(slice2.buffer, slice2.offset, bytes, slice1.length, slice2.length);

    return new Slice(bytes);
  }

  @Override
  public Pair<T1, T2> deserialize(Slice slice, MutableInt offset)
  {
    T1 first = serde1.deserialize(slice, offset);
    T2 second = serde2.deserialize(slice, offset);
    return new ImmutablePair<>(first, second);
  }

  @Override
  public Pair<T1, T2> deserialize(Slice slice)
  {
    return deserialize(slice, new MutableInt(0));
  }
}
