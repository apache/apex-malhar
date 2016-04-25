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
import com.datatorrent.api.Context;
import com.datatorrent.lib.io.block.BlockMetadata;

/**
 * FSFileSplitter extends {@link FileSplitterInput} to,
 * 1. Ignore files with extension "ignoreFilePatternRegularExp"
 * 2. Set sequencial read option on readers.
 */
public class FSFileSplitter extends FileSplitterInput
{
  private boolean sequencialFileRead;

  public FSFileSplitter()
  {
    super();
    super.setScanner(new FSScanner());
  }

  @Override
  protected BlockMetadata.FileBlockMetadata createBlockMetadata(FileMetadata fileMetadata)
  {
    BlockMetadata.FileBlockMetadata blockMetadta = new BlockMetadata.FileBlockMetadata(fileMetadata.getFilePath());
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
   * FSScanner extends {@link TimeBasedDirectoryScanner} to ignore temporary files
   * and files containing unsupported characters.
   */
  public static class FSScanner extends TimeBasedDirectoryScanner
  {
    private String unsupportedCharacter;
    private String ignoreFilePatternRegularExp;
    private transient Pattern ignoreRegex;

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      if (ignoreFilePatternRegularExp != null) {
        ignoreRegex = Pattern.compile(this.ignoreFilePatternRegularExp);
      }
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
      if (unsupportedCharacter != null) {
        return new Path(filePathStr).toUri().getPath().contains(unsupportedCharacter);
      }
      return false;
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

    public String getUnsupportedCharacter()
    {
      return unsupportedCharacter;
    }

    public void setUnsupportedCharacter(String unsupportedCharacter)
    {
      this.unsupportedCharacter = unsupportedCharacter;
    }
  }
}

