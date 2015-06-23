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
package com.datatorrent.contrib.hdht;


import com.google.common.base.Preconditions;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.Slice;

public class HDHTTestOperator extends AbstractSinglePortHDHTWriter<KeyValPair<byte[], byte[]>>
{
  @Override
  protected HDHTCodec<KeyValPair<byte[], byte[]>> getCodec()
  {
    return new BucketStreamCodec();
  }

  public static class BucketStreamCodec extends KryoSerializableStreamCodec<KeyValPair<byte[], byte[]>> implements HDHTCodec<KeyValPair<byte[], byte[]>>
  {
    private static final long serialVersionUID = 1L;
    private transient HDHTTestOperator operator;

    @Override
    public int getPartition(KeyValPair<byte[], byte[]> t)
    {
      int length = t.getKey().length;
      int hash = 0;
      for (int i = length-4; i > 0 && i < length; i++) {
        hash <<= 8;
        hash += t.getKey()[i];
      }
      return hash;
    }

    @Override
    public byte[] getKeyBytes(KeyValPair<byte[], byte[]> event)
    {
      return event.getKey();
    }

    @Override
    public byte[] getValueBytes(KeyValPair<byte[], byte[]> event)
    {
      return event.getValue();
    }

    @Override
    public KeyValPair<byte[], byte[]> fromKeyValue(Slice key, byte[] value)
    {
      return new KeyValPair<byte[], byte[]>(key.buffer, value);
    }
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    super.setup(arg0);
    Preconditions.checkNotNull(this.codec, "codec not set");
    Preconditions.checkNotNull(((BucketStreamCodec)this.codec).operator, "operator not set");
  }

}
