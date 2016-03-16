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
package com.datatorrent.lib.io.fs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;

/**
 * HDFSFileSplitter extends {@link FileSplitterInput} to,
 * 1. Add relative path to file metadata.
 * 2. Ignore HDFS temp files (files with extensions _COPYING_).
 * 3. Set sequencial read option on readers.
 */
public class HDFSFileSplitter extends FileSplitterInput
{
  private boolean sequencialFileRead;

  public HDFSFileSplitter()
  {
    super();
    super.setScanner(new HDFSScanner());
  }


  @Override
  protected FileBlockMetadata createBlockMetadata(FileMetadata fileMetadata)
  {
    FileBlockMetadata blockMetadta = new FileBlockMetadata(fileMetadata.getFilePath());
    blockMetadta.setReadBlockInSequence(sequencialFileRead);
    return blockMetadta;
  }

  public boolean isSequencialFileRead()
  {
    return sequencialFileRead;
  }

  public void setSequencialFileRead(boolean sequencialFileRead)
  {
    this.sequencialFileRead = sequencialFileRead;
  }

  /**
   * HDFSScanner extends {@link TimeBasedDirectoryScanner} to ignore HDFS temporary files
   * and files containing unsupported characters. 
   */
  public static class HDFSScanner extends TimeBasedDirectoryScanner
  {
    protected static final String UNSUPPORTED_CHARACTOR = ":";
    private String ignoreFilePatternRegularExp = ".*._COPYING_";
    private transient Pattern ignoreRegex;

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (containsUnsupportedCharacters(filePathStr) || isIgnoredFile(filePathStr)) {
        return false;
      }
      return accepted;
    }

    private boolean isIgnoredFile(String filePathStr)
    {
      String fileName = new Path(filePathStr).getName();
      if (ignoreRegex != null) {
        Matcher matcher = ignoreRegex.matcher(fileName);
        if (matcher.matches()) {
          return true;
        }
      }
      return false;
    }

    private boolean containsUnsupportedCharacters(String filePathStr)
    {
      return new Path(filePathStr).toUri().getPath().contains(UNSUPPORTED_CHARACTOR);
    }

    public String getIgnoreFilePatternRegularExp()
    {
      return ignoreFilePatternRegularExp;
    }

    public void setIgnoreFilePatternRegularExp(String ignoreFilePatternRegularExp)
    {
      this.ignoreFilePatternRegularExp = ignoreFilePatternRegularExp;
      this.ignoreRegex = Pattern.compile(ignoreFilePatternRegularExp);
    }
  }

}
