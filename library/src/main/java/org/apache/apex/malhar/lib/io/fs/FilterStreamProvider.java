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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * An interface for classes that want to provide filtering functionality for data being written to a
 * file. Multiple filters can be chained together using the chain filter provider.
 *
 * @since 2.1.0
 */
public interface FilterStreamProvider<F extends FilterOutputStream, S extends OutputStream>
{
  public FilterStreamContext<F> getFilterStreamContext(S outputStream) throws IOException;

  public void reclaimFilterStreamContext(FilterStreamContext<F> filterStreamContext);

  abstract class SimpleFilterReusableStreamProvider<F extends FilterOutputStream, S extends OutputStream> implements FilterStreamProvider<F, S>
  {

    private transient Map<OutputStream, FilterStreamContext<F>> reusableContexts = Maps.newHashMap();
    private transient Map<FilterStreamContext<F>, OutputStream> allocatedContexts = Maps.newHashMap();

    @Override
    public FilterStreamContext<F> getFilterStreamContext(S outputStream) throws IOException
    {
      FilterStreamContext<F> streamContext = reusableContexts.remove(outputStream);
      if (streamContext == null) {
        streamContext = createFilterStreamContext(outputStream);
      }
      allocatedContexts.put(streamContext, outputStream);
      return streamContext;
    }

    @Override
    public void reclaimFilterStreamContext(FilterStreamContext<F> filterStreamContext)
    {
      OutputStream outputStream = allocatedContexts.remove(filterStreamContext);
      if (outputStream != null) {
        reusableContexts.put(outputStream, filterStreamContext);
      }
    }

    protected abstract FilterStreamContext<F> createFilterStreamContext(OutputStream outputStream) throws IOException;
  }

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
      FilterChainStreamContext filterStreamContext = new FilterChainStreamContext();
      OutputStream currOutputStream = outputStream;
      for (int i = streamProviders.size() - 1; i >= 0; i-- ) {
        //for (FilterStreamProvider streamProvider : streamProviders) {
        FilterStreamProvider streamProvider = streamProviders.get(i);
        FilterStreamContext streamContext = streamProvider.getFilterStreamContext(currOutputStream);
        filterStreamContext.pushStreamContext(streamContext);
        currOutputStream = streamContext.getFilterStream();
      }
      return filterStreamContext;
    }

    @Override
    public void reclaimFilterStreamContext(FilterStreamContext<F> filterStreamContext)
    {
      FilterChainStreamContext filterChainContext = (FilterChainStreamContext)filterStreamContext;
      Collection<FilterStreamContext<?>> streamContexts = filterChainContext.getStreamContexts();
      Iterator<FilterStreamContext<?>> iterator = streamContexts.iterator();
      for (FilterStreamProvider streamProvider : streamProviders) {
        if (iterator.hasNext()) {
          streamProvider.reclaimFilterStreamContext(iterator.next());
        }
      }
    }

    private class FilterChainStreamContext extends FilterStreamContext.BaseFilterStreamContext
        implements FilterStreamContext
    {

      private List<FilterStreamContext<?>> streamContexts = new ArrayList<FilterStreamContext<?>>();

      public void pushStreamContext(FilterStreamContext<?> streamContext)
      {
        streamContexts.add(0, streamContext);
        filterStream = streamContext.getFilterStream();
      }

      public Collection<FilterStreamContext<?>> getStreamContexts()
      {
        return Collections.unmodifiableCollection(streamContexts);
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

