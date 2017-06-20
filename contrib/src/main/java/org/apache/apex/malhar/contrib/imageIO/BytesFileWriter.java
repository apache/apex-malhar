/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain imageInBytes copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.apex.malhar.contrib.imageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class BytesFileWriter extends AbstractFileOutputOperator<Data>
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BytesFileWriter.class);

  public String fileName;
  private boolean eof;

  @Override
  protected byte[] getBytesForTuple(Data tuple)
  {
    eof = true;
    return tuple.bytesImage;
  }

  @Override
  protected String getFileName(Data tuple)
  {
    fileName = tuple.fileName;
    LOG.info("fileName :" + fileName);
    return tuple.fileName;
  }

  @Override
  public void endWindow()
  {

    if (!eof) {
      LOG.debug("ERR no eof" + fileName);
      return;
    }
    if (null == fileName) {
      LOG.debug("ERR file name is null" + fileName);
      return;
    }
    try {
      finalizeFile(fileName);
      Thread.sleep(100);
    } catch (Exception e) {
      LOG.debug("Finalize err " + e.getMessage());
    }
    super.endWindow();
    eof = false;
    fileName = null;
  }


}
