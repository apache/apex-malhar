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

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.block.HDFSBlockMetadata;

public class HDFSFileSplitter extends FileSplitterInput
{
  private boolean sequencialFileRead;

  public HDFSFileSplitter()
  {
    super();
    super.setScanner(new HDFSScanner());
  }

  @Override
  protected FileMetadata createFileMetadata(FileInfo fileInfo)
  {
    return new HDFSFileMetaData(fileInfo.getFilePath());
  }

  @Override
  protected HDFSFileMetaData buildFileMetadata(FileInfo fileInfo) throws IOException
  {
    FileMetadata metadata = super.buildFileMetadata(fileInfo);
    HDFSFileMetaData hdfsFileMetaData = (HDFSFileMetaData) metadata;

    Path path = new Path(fileInfo.getFilePath());
    FileStatus status = getFileStatus(path);
    if (fileInfo.getDirectoryPath() == null) { // Direct filename is given as input.
      hdfsFileMetaData.setRelativePath(status.getPath().getName());
    } else {
      String relativePath = getRelativePathWithFolderName(fileInfo);
      hdfsFileMetaData.setRelativePath(relativePath);
    }
    return hdfsFileMetaData;
  }

  /*
   * As folder name was given to input for copy, prefix folder name to the sub items to copy.
   */
  private String getRelativePathWithFolderName(FileInfo fileInfo)
  {
    String parentDir = new Path(fileInfo.getDirectoryPath()).getName();
    return parentDir + File.separator + fileInfo.getRelativeFilePath();
  }

  @Override
  protected HDFSBlockMetadata createBlockMetadata(FileMetadata fileMetadata)
  {
    HDFSBlockMetadata blockMetadta = new HDFSBlockMetadata(fileMetadata.getFilePath());
    blockMetadta.setReadBlockInSequence(sequencialFileRead);
    return blockMetadta;
  }

  @Override
  protected HDFSBlockMetadata buildBlockMetadata(long pos, long lengthOfFileInBlock, int blockNumber, FileMetadata fileMetadata, boolean isLast)
  {
    FileBlockMetadata metadata = super.buildBlockMetadata(pos, lengthOfFileInBlock, blockNumber, fileMetadata, isLast);
    HDFSBlockMetadata blockMetadata = (HDFSBlockMetadata) metadata;
    return blockMetadata;
  }

  public boolean isSequencialFileRead()
  {
    return sequencialFileRead;
  }

  public void setSequencialFileRead(boolean sequencialFileRead)
  {
    this.sequencialFileRead = sequencialFileRead;
  }

  public static class HDFSScanner extends TimeBasedDirectoryScanner
  {
    protected final static String HDFS_TEMP_FILE = ".*._COPYING_";
    protected final static String UNSUPPORTED_CHARACTOR = ":";
    private transient Pattern ignoreRegex;

    @Override
    public void setup(OperatorContext context)
    {
      super.setup(context);
      ignoreRegex = Pattern.compile(HDFS_TEMP_FILE);
    }

    @Override
    protected boolean acceptFile(String filePathStr)
    {
      boolean accepted = super.acceptFile(filePathStr);
      if (containsUnsupportedCharacters(filePathStr) || isTempFile(filePathStr)) {
        return false;
      }
      return accepted;
    }

    private boolean isTempFile(String filePathStr)
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
  }

  public static class HDFSFileMetaData extends FileMetadata
  {
    private String relativePath;

    protected HDFSFileMetaData()
    {
      super();
    }

    public HDFSFileMetaData(@NotNull String filePath)
    {
      super(filePath);
    }

    public String getRelativePath()
    {
      return relativePath;
    }

    public void setRelativePath(String relativePath)
    {
      this.relativePath = relativePath;
    }

    @Override
    public String toString()
    {
      return "HDFSFileMetaData [relativePath=" + relativePath + ", getNumberOfBlocks()=" + getNumberOfBlocks() + ", getFileName()=" + getFileName() + ", getFileLength()=" + getFileLength() + ", isDirectory()=" + isDirectory() + "]";
    }

    public String getOutputRelativePath()
    {
      return relativePath;
    }

  }
}
