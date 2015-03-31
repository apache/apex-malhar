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

/**
 *
 */
public interface FilterStreamContext<F extends FilterOutputStream>
{
  public F getFilterStream();
  public void finalizeContext() throws IOException;

  /**
   * Base filter context that can be extended to build custom filters.
   * @param <F> The Filter output stream
   */
  public static abstract class BaseFilterStreamContext<F extends FilterOutputStream> implements FilterStreamContext<F>
  {
    protected F filterStream;

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

}
