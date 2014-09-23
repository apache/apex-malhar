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
package com.datatorrent.contrib.hds;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.base.Preconditions;

public class HDSTestOperator extends AbstractSinglePortHDSWriter<KeyValPair<byte[], byte[]>>
{
  @Override
  protected Class<? extends com.datatorrent.contrib.hds.AbstractSinglePortHDSWriter.HDSCodec<KeyValPair<byte[], byte[]>>> getCodecClass()
  {
    return BucketStreamCodec.class;
  }

  public static class BucketStreamCodec extends KryoSerializableStreamCodec<KeyValPair<byte[], byte[]>> implements HDSCodec<KeyValPair<byte[], byte[]>>
  {
    public HDSTestOperator operator;

    @Override
    public int getPartition(KeyValPair<byte[], byte[]> t)
    {
      int length = t.getKey().length;
      int hash = 0;
      for (int i = length-4; i > 0 && i < length; i++) {
        hash = hash << 8;
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
    public KeyValPair<byte[], byte[]> fromKeyValue(byte[] key, byte[] value)
    {
      return new KeyValPair<byte[], byte[]>(key, value);
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
