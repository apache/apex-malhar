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
package org.apache.apex.malhar.lib.io.fs;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;

/**
 * Filters for compression and encryption.
 *
 * @since 2.1.0
 */
public class FilterStreamCodec
{
  /**
   * The GZIP filter to use for compression
   */
  public static class GZIPFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<GZIPOutputStream>
  {
    public GZIPFilterStreamContext(OutputStream outputStream) throws IOException
    {
      filterStream = new GZIPOutputStream(outputStream);
    }

    @Override
    public void finalizeContext() throws IOException
    {
      filterStream.finish();
      //filterStream.close();
    }
  }

  /**
   * A provider for GZIP filter
   */
  public static class GZipFilterStreamProvider implements FilterStreamProvider<GZIPOutputStream, OutputStream>
  {
    @Override
    public FilterStreamContext<GZIPOutputStream> getFilterStreamContext(OutputStream outputStream) throws IOException
    {
      return new GZIPFilterStreamContext(outputStream);
    }

    @Override
    public void reclaimFilterStreamContext(FilterStreamContext<GZIPOutputStream> filterStreamContext)
    {

    }
  }

  /**
   * This filter should be used when cipher cannot be reused for example when writing to different output streams
   */
  public static class CipherFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<CipherOutputStream>
  {
    public CipherFilterStreamContext(OutputStream outputStream, Cipher cipher) throws IOException
    {
      filterStream = new CipherOutputStream(outputStream, cipher);
    }
  }

  /**
   * This provider is useful when writing to a single output stream so that the same cipher can be reused
   */
  public static class CipherSimpleStreamProvider implements FilterStreamProvider<CipherOutputStream, OutputStream>
  {
    private transient Cipher cipher;

    public Cipher getCipher()
    {
      return cipher;
    }

    public void setCipher(Cipher cipher)
    {
      this.cipher = cipher;
    }

    @Override
    public FilterStreamContext<CipherOutputStream> getFilterStreamContext(OutputStream outputStream) throws IOException
    {
      return new FilterStreamContext.SimpleFilterStreamContext<CipherOutputStream>(
          new CipherOutputStream(outputStream, cipher));
    }

    @Override
    public void reclaimFilterStreamContext(FilterStreamContext<CipherOutputStream> filterStreamContext)
    {

    }
  }

  public static class SnappyFilterStream extends FilterOutputStream
  {
    /**
     * Creates an output stream filter built on top of the specified
     * underlying output stream.
     *
     * @param out the underlying output stream to be assigned to
     *            the field <tt>this.out</tt> for later use, or
     *            <code>null</code> if this instance is to be
     *            created without an underlying stream.
     */
    public SnappyFilterStream(CompressionOutputStream out)
    {
      super(out);
    }

    public void finish() throws IOException
    {
      ((CompressionOutputStream)out).finish();
    }
  }

  public static class SnappyFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<SnappyFilterStream>
  {
    private int bufferSize = 256 * 1024;

    public void setBufferSize(int bufferSize)
    {
      this.bufferSize = bufferSize;
    }

    public SnappyFilterStreamContext(OutputStream outputStream) throws IOException
    {
      SnappyCodec codec = new SnappyCodec();
      codec.setConf(new Configuration());
      try {
        filterStream = new SnappyFilterStream(
            codec.createOutputStream(outputStream, new SnappyCompressor(bufferSize)));
      } catch (IOException e) {
        throw e;
      }
    }

    @Override
    public void finalizeContext() throws IOException
    {
      try {
        filterStream.finish();
      } catch (IOException e) {
        throw e;
      }
    }
  }

  /**
   * A provider for Snappy filter
   */
  public static class SnappyFilterStreamProvider implements FilterStreamProvider<SnappyFilterStream,
      OutputStream>
  {
    @Override
    public FilterStreamContext<SnappyFilterStream> getFilterStreamContext(OutputStream outputStream)
        throws IOException
    {
      return new SnappyFilterStreamContext(outputStream);
    }

    @Override
    public void reclaimFilterStreamContext(FilterStreamContext<SnappyFilterStream> filterStreamContext)
    {

    }
  }
}
