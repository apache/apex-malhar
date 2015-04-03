/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.io.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

/**
 * Filters for compression and encryption.
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
      return new FilterStreamContext.SimpleFilterStreamContext<CipherOutputStream>(new CipherOutputStream(outputStream, cipher));
    }

    @Override
    public void reclaimFilterStreamContext(FilterStreamContext<CipherOutputStream> filterStreamContext)
    {

    }
  }
}
