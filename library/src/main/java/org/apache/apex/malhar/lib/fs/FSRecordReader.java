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

package org.apache.apex.malhar.lib.fs;

import java.io.IOException;

import javax.validation.constraints.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.block.FSSliceReader;
import com.datatorrent.lib.io.block.ReaderContext;

/**
 * This operator can be used for reading records/tuples from Filesystem in
 * parallel (without ordering guarantees between tuples). Records can be
 * delimited (e.g. newline) or fixed width records. Output tuples are byte[].
 *
 * Typically, this operator will be connected to output of FileSplitterInput to
 * read records in parallel.
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class FSRecordReader extends FSSliceReader
{
  /**
   * Record reader mode decides how to split the records.
   */
  public static enum RECORD_READER_MODE
  {
    DELIMITED_RECORD, FIXED_WIDTH_RECORD;
  }

  /**
   * Criteria for record split
   */
  private RECORD_READER_MODE mode = RECORD_READER_MODE.DELIMITED_RECORD;

  /**
   * Length for fixed width record
   */
  private int recordLength;

  /**
   * Port to emit individual records/tuples as byte[]
   */
  public final transient DefaultOutputPort<byte[]> records = new DefaultOutputPort<byte[]>();

  /**
   * Initialize appropriate reader context based on mode selection
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD) {
      readerContext = createFixedWidthReaderContext();
    } else {
      readerContext = createDelimitedReaderContext();
    }
  }

  /**
   * Creates a recordReaderContext for FixedWidthRecords
   *
   * @return FixedBytesReaderContext
   */
  protected ReaderContext<FSDataInputStream> createFixedWidthReaderContext()
  {
    ReaderContext.FixedBytesReaderContext<FSDataInputStream> fixedBytesReaderContext = new ReaderContext.FixedBytesReaderContext<FSDataInputStream>();
    fixedBytesReaderContext.setLength(recordLength);
    return fixedBytesReaderContext;

  }

  /**
   * Creates a recordReaderContext for Delimited Records
   *
   * @return DelimitedRecordReaderContext
   */
  protected ReaderContext<FSDataInputStream> createDelimitedReaderContext()
  {
    return new ReaderContext.ReadAheadLineReaderContext<FSDataInputStream>();
  }

  /**
   * Read the block data and emit records based on reader context
   *
   * @param blockMetadata
   *          block
   * @throws IOException
   */
  protected void readBlock(BlockMetadata blockMetadata) throws IOException
  {
    readerContext.initialize(stream, blockMetadata, consecutiveBlock);
    ReaderContext.Entity entity;
    while ((entity = readerContext.next()) != null) {

      counters.getCounter(ReaderCounterKeys.BYTES).add(entity.getUsedBytes());

      byte[] record = entity.getRecord();

      if (record != null) {
        counters.getCounter(ReaderCounterKeys.RECORDS).increment();
        records.emit(record);
      }
    }
  }

  /**
   * Criteria for record split : FIXED_WIDTH_RECORD or DELIMITED_RECORD
   *
   * @param mode
   *          Mode
   */
  public void setMode(
      @Pattern(regexp = "FIXED_WIDTH_RECORD|DELIMITED_RECORD", flags = Pattern.Flag.CASE_INSENSITIVE) String mode)
  {
    this.mode = RECORD_READER_MODE.valueOf(mode.toUpperCase());
  }

  /**
   * Criteria for record split
   *
   * @return mode
   */
  public String getMode()
  {
    return mode.toString();
  }

  /**
   * Length for fixed width record
   *
   * @param recordLength
   */
  public void setRecordLength(int recordLength)
  {
    if (mode == RECORD_READER_MODE.FIXED_WIDTH_RECORD && recordLength <= 0) {
      throw new IllegalArgumentException("recordLength should be greater than 0.");
    }
    this.recordLength = recordLength;
  }

  /**
   * Length for fixed width record
   *
   * @return record length
   */
  public int getRecordLength()
  {
    return recordLength;
  }

}
