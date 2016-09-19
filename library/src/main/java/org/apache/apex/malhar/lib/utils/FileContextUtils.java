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
package org.apache.apex.malhar.lib.utils;

import java.net.URI;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import com.google.common.base.Preconditions;

/**
 * @since 3.4.0
 */
public class FileContextUtils
{
  public static FileContext getFileContext(@NotNull Path path) throws UnsupportedFileSystemException
  {
    return getFileContext(path, null);
  }

  public static FileContext getFileContext(@NotNull String path) throws UnsupportedFileSystemException
  {
    Preconditions.checkNotNull(path, "path");
    return getFileContext(new Path(path), null);
  }

  public static FileContext getFileContext(@NotNull String path, @Nullable Configuration conf)
    throws UnsupportedFileSystemException
  {
    Preconditions.checkNotNull(path, "path");
    return getFileContext(new Path(path), conf);
  }

  public static FileContext getFileContext(@NotNull Path path, @Nullable Configuration conf)
    throws UnsupportedFileSystemException
  {
    Preconditions.checkNotNull(path, "path");
    URI pathUri = path.toUri();

    if (pathUri.getScheme() != null) {
      return FileContext.getFileContext(pathUri, conf == null ? new Configuration() : conf);
    } else {
      return FileContext.getFileContext(conf == null ? new Configuration() : conf);
    }
  }
}
