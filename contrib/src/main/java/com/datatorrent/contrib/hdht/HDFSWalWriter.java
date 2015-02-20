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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHT.WALReader;

/**
 * HDFSWalWriter
 *
 * @since 2.0.0 
 */
public class HDFSWalWriter implements HDHT.WALWriter
{
  transient DataOutputStream out;
  long committedOffset;
  long unflushed;
  long bucketKey;
  String name;

  public HDFSWalWriter(HDHTFileAccess bfs, long bucketKey, String name) throws IOException
  {
    this.bucketKey = bucketKey;
    this.name = name;
    out = bfs.getOutputStream(bucketKey, name);
    unflushed = 0;
    committedOffset = 0;
  }

  @Override public void close() throws IOException
  {
    if (out != null)
    {
      out.flush();
      out.close();
    }
  }

  @Override
  public void append(Slice key, byte[] value) throws IOException
  {
    if (value == WALReader.DELETED) {
      out.writeInt(-key.length);
      out.write(key.buffer, key.offset, key.length);
    } else {
      out.writeInt(key.length);
      out.write(key.buffer, key.offset, key.length);
      out.writeInt(value.length);
      out.write(value);
    }
  }

  @Override public void flush() throws IOException
  {
    out.flush();
    if (out instanceof FSDataOutputStream) {
      ((FSDataOutputStream) out).hflush();
      ((FSDataOutputStream) out).hsync();
    }
    committedOffset = out.size();
    unflushed = 0;
  }

  @Override public long getUnflushedCount()
  {
    return unflushed;
  }

  @Override public long logSize()
  {
    return out.size();
  }

  @Override public long getCommittedLen()
  {
    return committedOffset;
  }

  @Override
  public String toString() {
    return "HDFSWalWritter Bucket " + bucketKey + " fileId " + name ;
  }

}
