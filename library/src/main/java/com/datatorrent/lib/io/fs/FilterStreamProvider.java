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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An interface for classes that want to provide filtering functionality for data being written to a
 * file. Multiple filters can be chained together using the chain filter provider.
 */
public interface FilterStreamProvider<F extends FilterOutputStream, S extends OutputStream>
{
  public FilterStreamContext<F> getFilterStreamContext(S outputStream) throws IOException;

  /**
   * The chain filter to use for chaining multiple filters.
   * @param <S> The original output stream
   */
  public static class FilterChainStreamProvider<F extends FilterOutputStream, S extends OutputStream> implements FilterStreamProvider<F, S>
  {
    private List<FilterStreamProvider<?,?>> streamProviders = new ArrayList<FilterStreamProvider<?, ?>>();
    
    public Collection<FilterStreamProvider<?,?>> getStreamProviders()
    {
      return Collections.unmodifiableList(streamProviders);
    }

    public void addStreamProvider(FilterStreamProvider<?,?> streamProvider)
    {
      streamProviders.add(streamProvider);
    }
    
    @Override
    public FilterStreamContext<F> getFilterStreamContext(S outputStream) throws IOException
    {
      return new FilterChainStreamContext(outputStream);
    }

    private class FilterChainStreamContext extends FilterStreamContext.BaseFilterStreamContext implements FilterStreamContext {
      
      private List<FilterStreamContext<?>> streamContexts = new ArrayList<FilterStreamContext<?>>();
      
      public FilterChainStreamContext(OutputStream outputStream) throws IOException {
        FilterOutputStream filterStream = null;
        OutputStream currOutputStream = outputStream;
        for (int i = streamProviders.size() - 1; i >= 0; i-- ) {
        //for (FilterStreamProvider streamProvider : streamProviders) {
          FilterStreamProvider streamProvider = streamProviders.get(i);
          FilterStreamContext streamContext = streamProvider.getFilterStreamContext(currOutputStream);
          streamContexts.add(streamContext);
          filterStream = streamContext.getFilterStream();
          currOutputStream = filterStream;
        }
        this.filterStream = filterStream;
      }

      @Override
      public void finalizeContext() throws IOException
      {
        for (FilterStreamContext streamContext : streamContexts) {
        //for (int i = streamContexts.size() - 1; i >= 0; i --) {
          //FilterStreamContext streamContext = streamContexts.get(i);
          streamContext.finalizeContext();
        }
      }
    }

  }
}

