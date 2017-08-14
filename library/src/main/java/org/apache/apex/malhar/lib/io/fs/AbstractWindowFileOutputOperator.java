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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This output operator writes the data received in each window to a file exactly once.
 * All the data received by this operator within a window is written out to a file whose name is the same as the id of the window.
 *
 * @displayName FS Window Writer
 * @category Output
 * @tags fs, file, output operator
 *
 * @since 1.0.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractWindowFileOutputOperator<INPUT> extends AbstractFileOutputOperator<INPUT>
{
  private transient String windowIdString;

  public AbstractWindowFileOutputOperator()
  {
    maxOpenFiles = 1;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    windowIdString = Long.toString(windowId);
  }

  @Override
  public void endWindow()
  {
    endOffsets.remove(windowIdString);
    streamsCache.invalidate(windowIdString);
  }

  @Override
  protected String getFileName(INPUT tuple)
  {
    return windowIdString;
  }

  @Override
  public void setMaxLength(long maxLength)
  {
    throw new UnsupportedOperationException("This property cannot be set on this operator.");
  }

  @Override
  public void setMaxOpenFiles(int maxOpenFiles)
  {
    throw new UnsupportedOperationException("This property cannot be set on this operator.");
  }

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWindowFileOutputOperator.class);
}
