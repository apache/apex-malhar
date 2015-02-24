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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Interface and base class for implementations that want to provide filtering functionality to data being written to
 * file. Multiple filters can be chained together using the chain filter context. 
 */
public interface FilterStreamContext<F extends FilterOutputStream, S extends OutputStream>
{
  public void setup(S outputStream) throws IOException;
  public F getFilterStream();
  public void finalizeContext() throws IOException;

  /**
   * Base filter context that can be extended to build custom filters.
   * @param <F> The Filter output stream
   * @param <S> The original output stream
   */
  public static abstract class BaseFilterStreamContext<F extends FilterOutputStream, S extends OutputStream> implements FilterStreamContext<F, S>
  {
    protected F filterStream;

    @Override
    public void setup(S outputStream) throws IOException
    {
      filterStream = createFilterStream(outputStream);
    }

    @Override
    public F getFilterStream()
    {
      return filterStream;
    }

    @Override
    public void finalizeContext() throws IOException
    {
      filterStream.flush();
    }
    
    protected abstract F createFilterStream(S outputStream) throws IOException;
    
  }

  public static abstract class BaseOutputFilterStreamContext<F extends FilterOutputStream> extends BaseFilterStreamContext<F, OutputStream>
  {
  }

  /**
   * The chain filter to use for chaining multiple filters.
   * @param <S> The original output stream
   */
  public static class FilterChainStreamContext<S extends OutputStream> extends BaseFilterStreamContext<FilterOutputStream, S>
  {
    private List<FilterStreamContext<?,?>> streamContexts = new ArrayList<FilterStreamContext<?,?>>();
    
    public List<FilterStreamContext<?,?>> getStreamContexts()
    {
      return streamContexts;
    }

    public void addStreamContext(FilterStreamContext<?,?> streamContext)
    {
      streamContexts.add(streamContext);
    }
    
    @Override
    public FilterOutputStream createFilterStream(OutputStream outputStream) throws IOException
    {
      FilterOutputStream filterStream = null;
      OutputStream currOutputStream = outputStream;
      for (int i = streamContexts.size() - 1; i >= 0; i-- ) {
        FilterStreamContext streamContext = streamContexts.get(i);
        streamContext.setup(currOutputStream);
        filterStream = streamContext.getFilterStream();
        currOutputStream = filterStream;
      }
      return filterStream;
    }

    @Override
    public void finalizeContext() throws IOException
    {
      for (FilterStreamContext streamContext : streamContexts) {
        streamContext.finalizeContext();
      }
    }
  }
}
