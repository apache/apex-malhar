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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * HDFSWalReader
 *
 * @since 2.0.0
 */
public class HDFSWalReader implements HDHT.WALReader
{
  DataInputStream in;
  private boolean eof = false;
  MutableKeyValue pair = null;
  String name;

  public HDFSWalReader(HDHTFileAccess bfs, long bucketKey, String name) throws IOException
  {
    this.name = name;
    in = bfs.getInputStream(bucketKey, name);
  }

  @Override public void close() throws IOException
  {
    if (in != null) {
      in.close();
    }
  }

  @Override public void seek(long offset) throws IOException
  {
    in.skipBytes((int) offset);
  }

  @Override public boolean advance() throws IOException
  {
    if (eof)
      return false;

    try {
      boolean isDelete = false;
      int keyLen = in.readInt();
      if (keyLen < 0) {
        keyLen = -keyLen;
        isDelete = true;
      }

      byte[] key = new byte[keyLen];
      in.readFully(key);

      byte[] value;
      if (!isDelete) {
        int valLen = in.readInt();
        value = new byte[valLen];
        in.readFully(value);
      } else {
        value = DELETED;
      }

      pair =  new MutableKeyValue(key, value);
      return true;
    } catch (EOFException ex) {
      eof = true;
      pair = null;
      return false;
    }
  }

  @Override public MutableKeyValue get() {
    return pair;
  }

}
