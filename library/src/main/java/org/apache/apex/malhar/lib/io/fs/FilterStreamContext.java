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

/**
 * An encapsulation of the filter stream that provides different methods to handle the stream.
 *
 * @since 2.1.0
 */
public interface FilterStreamContext<F extends FilterOutputStream>
{
  /**
   * Get the underlying filter stream encapsulated by the context
   * @return The filter stream
   */
  public F getFilterStream();

  /**
   * Finalize the context and write any pending data. The context may not be usable after this.
   * @throws IOException
   */
  public void finalizeContext() throws IOException;

  /**
   * Base filter context that can be extended to build custom filters.
   * @param <F> The Filter output stream
   */
  abstract class BaseFilterStreamContext<F extends FilterOutputStream> implements FilterStreamContext<F>
  {
    protected transient F filterStream;

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

  }

  public static class SimpleFilterStreamContext<F extends FilterOutputStream> extends BaseFilterStreamContext<F>
  {
    public SimpleFilterStreamContext(F filterStream)
    {
      this.filterStream = filterStream;
    }
  }

}
