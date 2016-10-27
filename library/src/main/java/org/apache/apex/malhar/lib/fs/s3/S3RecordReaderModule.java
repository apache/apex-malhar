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

package org.apache.apex.malhar.lib.fs.s3;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;

import com.datatorrent.lib.io.fs.S3BlockReader;

/**
 * This module is used for reading records/tuples from S3. Records can be read
 * in parallel using multiple partitions of record reader operator. (Ordering is
 * not guaranteed when records are read in parallel)
 *
 * Input S3 directory is scanned at specified interval to poll for new data.
 *
 * The module reads data in parallel, following parameters can be configured
 * <br/>
 * 1. files: List of file(s)/directories to read. files would be in the form of
 * SCHEME://AccessKey:SecretKey@BucketName/FileOrDirectory ,
 * SCHEME://AccessKey:SecretKey@BucketName/FileOrDirectory , .... where SCHEME
 * is the protocal scheme for the file system. AccessKey is the AWS access key
 * and SecretKey is the AWS Secret Key<br/>
 * 2. filePatternRegularExp: Files with names matching given regex will be read
 * <br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in
 * input directory<br/>
 * 4. recursive: if true, scan input directories recursively<br/>
 * 5. blockSize: block size used to read input blocks of file, default value
 * 64MB<br/>
 * 6. overflowBlockSize: For delimited records, this value represents the
 * additional data that needs to be read to find the delimiter character for
 * last record in a block. This should be set to approximate record size in the
 * file, default value is 1MB<br/>
 * 7. sequentialFileRead: if true, then each reader partition will read
 * different file. <br/>
 * instead of reading different offsets of the same file. <br/>
 * (File level parallelism instead of block level parallelism)<br/>
 * 8. blocksThreshold: number of blocks emitted per window<br/>
 * 9. minReaders: Minimum number of block readers for dynamic partitioning<br/>
 * 10. maxReaders: Maximum number of block readers for dynamic partitioning<br/>
 * 11. repartitionCheckInterval: Interval for re-evaluating dynamic
 * partitioning<br/>
 * different file. <br/>
 * 12. s3EndPoint: Optional parameter used to specify S3 endpoint to use
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3RecordReaderModule extends FSRecordReaderModule
{
  /**
   * Endpoint for S3
   */
  private String s3EndPoint;
  @Min(0)
  private int overflowBlockSize;

  /**
   * Creates an instance of Record Reader
   *
   * @return S3RecordReader instance
   */
  @Override
  public S3RecordReader createRecordReader()
  {
    S3RecordReader s3RecordReader = new S3RecordReader();
    s3RecordReader.setBucketName(S3BlockReader.extractBucket(getFiles()));
    s3RecordReader.setAccessKey(S3BlockReader.extractAccessKey(getFiles()));
    s3RecordReader.setSecretAccessKey(S3BlockReader.extractSecretAccessKey(getFiles()));
    s3RecordReader.setEndPoint(s3EndPoint);
    s3RecordReader.setMode(this.getMode());
    s3RecordReader.setRecordLength(this.getRecordLength());
    if (overflowBlockSize != 0) {
      s3RecordReader.setOverflowBufferSize(overflowBlockSize);
    }
    return s3RecordReader;
  }

  /**
   * Set the S3 endpoint to use
   *
   * @param s3EndPoint
   */
  public void setS3EndPoint(String s3EndPoint)
  {
    this.s3EndPoint = s3EndPoint;
  }

  /**
   * Returns the s3 endpoint
   *
   * @return s3EndPoint
   */
  public String getS3EndPoint()
  {
    return s3EndPoint;
  }

  /**
   * additional data that needs to be read to find the delimiter character for
   * last record in a block. This should be set to approximate record size in
   * the file, default value 1MB
   *
   * @param overflowBlockSize
   */
  public void setOverflowBlockSize(int overflowBlockSize)
  {
    this.overflowBlockSize = overflowBlockSize;
  }

  /**
   * returns the overflow block size
   *
   * @return overflowBlockSize
   */
  public int getOverflowBlockSize()
  {
    return overflowBlockSize;
  }
}
