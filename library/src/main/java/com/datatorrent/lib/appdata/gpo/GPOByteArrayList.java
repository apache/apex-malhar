/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.gpo;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteCollection;
import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.bytes.ByteList;

import java.util.Collection;
import java.util.Iterator;

/**
 * This is a helper class which stores primitive bytes in an array list. This is useful
 * for serialization and deserialization.
 */
public class GPOByteArrayList extends ByteArrayList
{
  private static final long serialVersionUID = 201503091653L;

  protected GPOByteArrayList(byte[] a, boolean dummy)
  {
    super(a, dummy);
  }

  public GPOByteArrayList(int capacity)
  {
    super(capacity);
  }

  public GPOByteArrayList()
  {
  }

  public GPOByteArrayList(Collection<? extends Byte> c)
  {
    super(c);
  }

  public GPOByteArrayList(ByteCollection c)
  {
    super(c);
  }

  public GPOByteArrayList(ByteList l)
  {
    super(l);
  }

  public GPOByteArrayList(byte[] a)
  {
    super(a);
  }

  public GPOByteArrayList(byte[] a, int offset, int length)
  {
    super(a, offset, length);
  }

  public GPOByteArrayList(Iterator<? extends Byte> i)
  {
    super(i);
  }

  public GPOByteArrayList(ByteIterator i)
  {
    super(i);
  }

  public boolean add(byte[] bytes)
  {
    for(int byteCounter = 0;
        byteCounter < bytes.length;
        byteCounter++) {
      this.add(bytes[byteCounter]);
    }

    return true;
  }
}
