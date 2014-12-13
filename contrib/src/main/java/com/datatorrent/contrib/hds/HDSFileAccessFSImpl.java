/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hds;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;

import com.datatorrent.common.util.DTThrowable;

/**
 * Hadoop file system backed store.
 */
abstract public class HDSFileAccessFSImpl implements HDSFileAccess
{
  @NotNull
  private String basePath;
  protected transient FileSystem fs;

  public HDSFileAccessFSImpl()
  {
  }


  private Set<HDSFileExporter> fileExporters = Sets.newHashSet();

  @Override
  public void registerExporter(HDSFileExporter exporter){
    fileExporters.add(exporter);
  }

  public void clearExporters() {
    fileExporters.clear();
  }

  @Override
  public void exportFiles(long bucketKey, Set<String> filesAdded, Set<String> filesToDelete) throws IOException {
    for(HDSFileExporter exporter: fileExporters) {
      exporter.exportFiles(this, bucketKey, filesAdded, filesToDelete);
    }
  }

  public String getBasePath()
  {
    return basePath;
  }
  public void setBasePath(String path)
  {
    this.basePath = path;
  }

  protected Path getFilePath(long bucketKey, String fileName) {
    return new Path(getBucketPath(bucketKey), fileName);
  }

  protected Path getBucketPath(long bucketKey)
  {
    return new Path(basePath, Long.toString(bucketKey));
  }

  @Override
  public long getFileSize(long bucketKey, String fileName) throws IOException {
    return fs.getFileStatus(getFilePath(bucketKey, fileName)).getLen();
  }

  @Override
  public void close() throws IOException
  {
    fs.close();
  }

  @Override
  public void init()
  {
    if (fs == null) {
      Path dataFilePath = new Path(basePath);
      try {
        fs = FileSystem.newInstance(dataFilePath.toUri(), new Configuration());
      } catch (IOException e) {
        DTThrowable.rethrow(e);
      }
    }
  }

  @Override
  public void delete(long bucketKey, String fileName) throws IOException
  {
    fs.delete(getFilePath(bucketKey, fileName), true);
  }

  @Override
  public FSDataOutputStream getOutputStream(long bucketKey, String fileName) throws IOException
  {
    Path path = getFilePath(bucketKey, fileName);
    return fs.create(path, true);
  }

  @Override
  public FSDataInputStream getInputStream(long bucketKey, String fileName) throws IOException
  {
    return fs.open(getFilePath(bucketKey, fileName));
  }

  @Override
  public void rename(long bucketKey, String fromName, String toName) throws IOException
  {
    FileContext fc = FileContext.getFileContext(fs.getUri());
    Path bucketPath = getBucketPath(bucketKey);
    // file context requires absolute path
    if (!bucketPath.isAbsolute()) {
      bucketPath = new Path(fs.getWorkingDirectory(), bucketPath);
    }
    fc.rename(new Path(bucketPath, fromName), new Path(bucketPath, toName), Rename.OVERWRITE);
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "[basePath=" + basePath + "]";
  }

}
