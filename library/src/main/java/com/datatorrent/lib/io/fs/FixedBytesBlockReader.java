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

import java.io.IOException;

import javax.validation.constraints.Min;

import com.datatorrent.common.util.Slice;

/**
 * An {@link AbstractBlockReader} which emits fixed-size byte-arrays wrapped in {@link Slice}.<br/>
 * This reader doesn't read beyond the block boundary therefore the last byte-array could be smaller.<br/>
 */
public class FixedBytesBlockReader extends AbstractBlockReader<Slice>
{
  @Min(1)
  protected int length;

  protected final transient Entity entity;

  public FixedBytesBlockReader()
  {
    super();
    length = 100;
    entity = new Entity();
  }

  @Override
  protected Entity readEntity(FileSplitter.BlockMetadata blockMetadata, long blockOffset) throws IOException
  {
    entity.clear();
    int bytesToRead = length;
    if (blockOffset + length >= blockMetadata.getLength()) {
      bytesToRead = (int) (blockMetadata.getLength() - blockOffset);
    }
    byte[] record = new byte[bytesToRead];
    inputStream.read(blockOffset, record, 0, bytesToRead);
    entity.usedBytes = bytesToRead;
    entity.record = record;

    return entity;
  }

  @Override
  protected Slice convertToRecord(byte[] bytes)
  {
    return new Slice(bytes);
  }

  @Override
  protected boolean isRecordValid(Slice record)
  {
    return true;
  }

  /**
   * Sets the length of each record.
   *
   * @param length fixed length of each record.
   */
  public void setLength(int length)
  {
    this.length = length;
  }

  /**
   * @return the length of record.
   */
  public int getLength()
  {
    return this.length;
  }
}
