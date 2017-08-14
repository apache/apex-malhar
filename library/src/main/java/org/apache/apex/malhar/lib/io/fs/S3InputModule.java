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

import org.apache.apex.malhar.lib.io.block.FSSliceReader;

/**
 * S3InputModule is used to read files/list of files (or directory) from S3 bucket. <br/>
 * Module emits, <br/>
 * 1. FileMetadata 2. BlockMetadata 3. Block Bytes.<br/><br/>
 * For more info about S3 scheme protocals, please have a look at
 * <a href="https://wiki.apache.org/hadoop/AmazonS3">https://wiki.apache.org/hadoop/AmazonS3.</a>
 *
 * The module reads data in parallel, following parameters can be configured<br/>
 * 1. files: List of file(s)/directories to read. files would be in the form of
 *           SCHEME://AccessKey:SecretKey@BucketName/FileOrDirectory ,
 *           SCHEME://AccessKey:SecretKey@BucketName/FileOrDirectory , ....
 *           where SCHEME is the protocal scheme for the file system.
 *                 AccessKey is the AWS access key and SecretKey is the AWS Secret Key<br/>
 * 2. filePatternRegularExp: Files names matching given regex will be read<br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in input directory<br/>
 * 4. recursive: if scan recursively input directories<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. readersCount: count of readers to read input file<br/>
 * 7. sequentialFileRead: Is emit file blocks in sequence?
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3InputModule extends FSInputModule
{
  /**
   * Creates the block reader for reading s3 blocks
   * @return S3BlockReader
   */
  @Override
  public FSSliceReader createBlockReader()
  {
    // Set the s3 bucket name, accessKey, SecretAccessKey to the block reader
    S3BlockReader reader = new S3BlockReader();
    reader.setBucketName(S3BlockReader.extractBucket(getFiles()));
    reader.setAccessKey(S3BlockReader.extractAccessKey(getFiles()));
    reader.setSecretAccessKey(S3BlockReader.extractSecretAccessKey(getFiles()));
    return reader;
  }
}
