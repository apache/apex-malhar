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
public abstract class AbstractFSWindowWriter<INPUT, OUTPUT> extends AbstractFSWriter<INPUT, OUTPUT>
{
  private static final long serialVersionUID = 201405201214L;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWindowWriter.class);

  private transient String windowIdString;

  public AbstractFSWindowWriter()
  {
    append = false;
    maxOpenFiles = 1;
  }

  @Override
  public final void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    windowIdString = Long.toString(windowId);
  }

  @Override
  public final void endWindow()
  {
    endOffsets.remove(getFileName(null));
  }

  @Override
  protected final String getFileName(INPUT tuple)
  {
    return windowIdString;
  }

  @Override
  public final void setAppend(boolean append)
  {
    throw new UnsupportedOperationException("This property cannot be set on this operator.");
  }

  @Override
  public final void setMaxLength(long maxLength)
  {
    throw new UnsupportedOperationException("This property cannot be set on this operator.");
  }

  @Override
  public final void setMaxOpenFiles(int maxOpenFiles)
  {
    throw new UnsupportedOperationException("This property cannot be set on this operator.");
  }
}
