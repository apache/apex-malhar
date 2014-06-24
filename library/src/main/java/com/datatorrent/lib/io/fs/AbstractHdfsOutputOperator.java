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

import org.apache.hadoop.fs.Path;

/**
 * Adapter for writing to HDFS
 * <p>
 * Serializes tuples into a HDFS file<br>
 * Tuples are written to a single HDFS file or multiple HDFS files, with the option to specify size based file rolling,
 * using place holders in the file path pattern.<br>
 * Example file path pattern : file:///mydir/adviews.out.%(operatorId).part-%(partIndex). where operatorId and partIndex
 * are place holders.
 * </p>
 *
 * @since 0.9.4
 * @deprecated
 */
@Deprecated
public abstract class AbstractHdfsOutputOperator<T> extends AbstractHdfsRollingFileOutputOperator<T>
{
  /**
   * The file name. This can be a relative path for the default file system or fully qualified URL as accepted by (
   * {@link org.apache.hadoop.fs.Path}). For splits with per file size limit, the name needs to contain substitution
   * tokens to generate unique file names. Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   *
   * @param filePathPattern
   * The pattern of the output file
   * @deprecated
   */
  @Deprecated
  public void setFilePathPattern(String filePathPattern)
  {
    this.filePath = filePathPattern;
  }

  /**
   * This returns the pattern of the output file
   *
   * @return
   * @deprecated 
   */
  @Deprecated
  public String getFilePathPattern()
  {
    return this.filePath;
  }

}