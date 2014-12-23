/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.AbstractSinglePortHDSWriter;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class GenericAggregateCodec extends KryoSerializableStreamCodec<GenericAggregate> implements AbstractSinglePortHDSWriter.HDSCodec<GenericAggregate>
{
  public DimensionStoreOperator operator;

  @Override
  public byte[] getKeyBytes(GenericAggregate aggr)
  {
    return operator.serializer.getKey(aggr);
  }

  @Override
  public byte[] getValueBytes(GenericAggregate aggr)
  {
    return operator.serializer.getValue(aggr);
  }

  @Override
  public GenericAggregate fromKeyValue(Slice key, byte[] value)
  {
    GenericAggregate aggr = operator.serializer.fromBytes(key, value);
    return aggr;
  }

  @Override
  public int getPartition(GenericAggregate aggr)
  {
    final int prime = 31;
    int hashCode = 1;
    for(Object o : aggr.keys)
    {
      if (o != null) {
        hashCode = hashCode * prime + o.hashCode();
      }
    }
    return hashCode;
  }
}
