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
package org.apache.apex.malhar.lib.io.block;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.StatsListener;
import com.datatorrent.netlet.util.Slice;

/**
 * An {@link AbstractFSBlockReader} which emits fixed-size byte-arrays wrapped in {@link Slice}.<br/>
 *
 * @category Input
 * @tags fs
 *
 * @since 2.1.0
 */
@StatsListener.DataQueueSize
public class FSSliceReader extends AbstractFSBlockReader<Slice>
{
  public FSSliceReader()
  {
    super();
    this.readerContext = new ReaderContext.FixedBytesReaderContext<>();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (basePath != null && this.readerContext instanceof ReaderContext.FixedBytesReaderContext) {
      ((ReaderContext.FixedBytesReaderContext)this.readerContext).setLength((int)fs.getDefaultBlockSize(new Path(basePath)));
    }
  }

  @Override
  protected Slice convertToRecord(byte[] bytes)
  {
    return new Slice(bytes);
  }
}
