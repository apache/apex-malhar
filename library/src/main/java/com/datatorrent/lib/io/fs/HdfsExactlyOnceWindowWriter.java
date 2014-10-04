/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output operator writes the data received in each window to a file exactly once.
 * All the data received by this operator within a window is written out to a file whose name is the same as the id of the window.
 *
 * @since 1.0.2
 */
public class HdfsExactlyOnceWindowWriter extends AbstractHDFSExactlyOnceWriter<String, String>
{
  private static final long serialVersionUID = 201405201214L;
  private static final Logger LOG = LoggerFactory.getLogger(HdfsExactlyOnceWindowWriter.class);

  private transient String windowIdString;

  public HdfsExactlyOnceWindowWriter()
  {
    this.setAppend(false);
    this.setMaxOpenFiles(1);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    windowIdString = Long.toString(windowId);
  }

  @Override
  protected byte[] getBytesForTuple(String t)
  {
    return (t + "\n").getBytes();
  }

  @Override
  protected String convert(String tuple)
  {
    throw new UnsupportedOperationException("This operator does not support this feature.");
  }

  @Override
  protected String getFileName(String tuple)
  {
    return windowIdString;
  }
}
