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
package org.apache.apex.malhar.lib.fileaccess;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Hadoop file system backed store.
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
public abstract class FileAccessFSImpl implements FileAccess
{
  @NotNull
  private String basePath;
  protected transient FileSystem fs;

  public FileAccessFSImpl()
  {
  }

  public String getBasePath()
  {
    return basePath;
  }

  public void setBasePath(String path)
  {
    this.basePath = path;
  }

  protected Path getFilePath(long bucketKey, String fileName)
  {
    return new Path(getBucketPath(bucketKey), fileName);
  }

  protected Path getBucketPath(long bucketKey)
  {
    return new Path(basePath, Long.toString(bucketKey));
  }

  @Override
  public long getFileSize(long bucketKey, String fileName) throws IOException
  {
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
  public boolean exists(long bucketKey, String fileName) throws IOException
  {
    return fs.exists(getFilePath(bucketKey, fileName));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(long bucketKey) throws IOException
  {
    Path bucketPath = getBucketPath(bucketKey);
    return fs.exists(bucketPath) ? fs.listFiles(bucketPath, true) : null;
  }

  @Override
  public void deleteBucket(long bucketKey) throws IOException
  {
    fs.delete(getBucketPath(bucketKey), true);
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "[basePath=" + basePath + "]";
  }

}
