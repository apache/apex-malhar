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
package org.apache.apex.malhar.lib.wal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import com.google.common.base.Preconditions;

import com.datatorrent.lib.fileaccess.FileAccess;

/**
 * WAL implementation which allows writing entries to single file.
 * the pointer type is the offset in the file. The entry is serialized into byte array and
 * first length of the entry is written followed by the serialized bytes.
 * @param <T>
 */
public class FSWal<T> implements WAL<T, Long>
{
  transient long bucketKey;
  private transient String name;
  private transient FileAccess fa;
  private transient WAL.Serde<T> serde;

  public FSWal(FileAccess fa, WAL.Serde<T> serde, long bucketKey, String name)
  {
    this.name = name;
    this.serde = serde;
    this.fa = fa;
    this.bucketKey = bucketKey;
  }

  @Override
  public WALReader getReader() throws IOException
  {
    return new FSWALReader();
  }

  @Override
  public WALWriter getWriter() throws IOException
  {
    return new FSWALWriter();
  }

  public class FSWALReader implements WAL.WALReader<T, Long>
  {
    private transient DataInputStream in;
    protected T entry = null;
    protected long offset;
    protected boolean eof = false;

    public FSWALReader() throws IOException
    {
      in = fa.getInputStream(bucketKey, name);
    }

    @Override
    public void close() throws IOException
    {
      if (in != null) {
        in.close();
      }
    }

    @Override
    public void seek(Long offset) throws IOException
    {
      in.skipBytes(offset.intValue());
      this.offset = offset;
    }

    @Override
    public boolean advance() throws IOException
    {
      if (eof) {
        return false;
      }

      try {
        int len = in.readInt();
        Preconditions.checkState(len >= 0);

        byte[] data = new byte[len];
        in.readFully(data);

        entry = serde.fromBytes(data);
        offset += data.length;
        return true;
      } catch (EOFException ex) {
        eof = true;
        entry = null;
        return false;
      }
    }

    @Override
    public T get()
    {
      return entry;
    }

    @Override
    public Long getOffset()
    {
      return offset;
    }
  }

  public class FSWALWriter implements WAL.WALWriter<T, Long>
  {
    private transient DataOutputStream out;

    public FSWALWriter() throws IOException
    {
      out = fa.getOutputStream(bucketKey, name);
    }

    @Override
    public void close() throws IOException
    {
      if (out != null) {
        out.flush();
        out.close();
      }
    }

    @Override
    public int append(T entry) throws IOException
    {
      byte[] slice = serde.toBytes(entry);
      out.writeInt(slice.length);
      out.write(slice);
      return slice.length + 4;
    }

    @Override
    public void flush() throws IOException
    {
      out.flush();
      if (out instanceof FSDataOutputStream) {
        ((FSDataOutputStream)out).hflush();
        ((FSDataOutputStream)out).hsync();
      }
    }

    @Override
    public Long getOffset()
    {
      return (long)out.size();
    }

    @Override
    public String toString()
    {
      return "HDFSWalWritter Bucket " + bucketKey + " fileId " + name;
    }
  }
}
