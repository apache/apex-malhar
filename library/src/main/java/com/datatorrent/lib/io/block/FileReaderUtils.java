/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.io.block;

import java.net.URI;

import com.google.common.base.Splitter;

public class FileReaderUtils
{

  /**
   * Converts Scheme part of the URI to lower case. Multiple URI can be comma separated. If no scheme is there, no
   * change is made.
   *
   * @param
   * @return String with scheme part as lower case
   */
  public static String convertSchemeToLowerCase(String uri)
  {
    if (uri == null) {
      return null;
    }
    StringBuilder inputMod = new StringBuilder();
    for (String f : Splitter.on(",").omitEmptyStrings().split(uri)) {
      String scheme = URI.create(f).getScheme();
      if (scheme != null) {
        inputMod.append(f.replaceFirst(scheme, scheme.toLowerCase()));
      } else {
        inputMod.append(f);
      }
      inputMod.append(",");
    }
    inputMod.setLength(inputMod.length() - 1);
    return inputMod.toString();
  }
}
