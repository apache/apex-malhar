/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io;

import java.net.URI;
import javax.validation.constraints.NotNull;

/**
 * Sends tuple as POST with JSON content to the given URL.
 * <p>
 * Data of type {@link java.util.Map} is converted to JSON. All other types are sent in their {@link Object#toString()} representation.<br>
 * <br>
 * </p>
 * @displayName Http Output
 * @category io
 * @tags http, output
 *
 * @param <T>
 * @since 0.3.2
 * @deprecated
 */
@Deprecated
public class HttpOutputOperator<T> extends HttpPostOutputOperator<T>
{
  @Deprecated
  public void setResourceURL(URI url)
  {
    if (!url.isAbsolute()) {
      throw new IllegalArgumentException("URL is not absolute: " + url);
    }
    this.url = url.toString();
  }

}
