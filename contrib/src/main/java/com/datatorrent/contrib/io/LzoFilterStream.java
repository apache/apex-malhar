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

package com.datatorrent.contrib.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoConstraint;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzopOutputStream;

import com.datatorrent.lib.io.fs.FilterStreamContext;
import com.datatorrent.lib.io.fs.FilterStreamProvider;

public class LzoFilterStream
{

  public static class LZOFilterStreamContext extends FilterStreamContext.BaseFilterStreamContext<LzopFilterStream>
  {
    public LZOFilterStreamContext(OutputStream outputStream) throws IOException
    {
      filterStream = new LzopFilterStream(new LzopOutputStream(outputStream, LzoLibrary.getInstance().newCompressor(LzoAlgorithm.LZO1X, LzoConstraint.COMPRESSION)));
    }
  }

  static class LzopFilterStream extends FilterOutputStream
  {
    public LzopFilterStream(OutputStream outputStream)
    {
      super(outputStream);
    }
  }

  /**
   * A provider for LZO filter The file has to be rewritten from beginning when there is operator failure. The operates
   * on entries file and not on file parts.
   */
  public static class LZOFilterStreamProvider extends FilterStreamProvider.SimpleFilterReusableStreamProvider<LzopFilterStream, OutputStream>
  {
    @Override
    protected FilterStreamContext<LzopFilterStream> createFilterStreamContext(OutputStream outputStream) throws IOException
    {
      return new LZOFilterStreamContext(outputStream);
    }
  }
}
