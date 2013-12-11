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
package com.datatorrent.contrib.netflow;

/**
 * This is a util class
 *
 * @since 0.9.2
 */
public class Util
{
    static public final long to_number(byte[] p, int off, int len)
  {
    long ret = 0;
    int done = off + len;
    for (int i = off; i < done; i++)
      ret = ((ret << 8) & 0xffffffff) + (p[i] & 0xff);

    return ret;
  }
    
  public static final String toString(byte[] p, int off, int len)
  {
    byte[] output = new byte[len];
    System.arraycopy(p, off, output, 0, len);
    return new String(output);
  }
}
