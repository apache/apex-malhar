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

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import java.io.*;

import java.util.regex.Pattern;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FSInputOperator<T> extends AbstractThroughputFSDirectoryInputOperator<T>
{
  private static final Logger LOG = LoggerFactory.getLogger(FSInputOperator.class);
    
  public final transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  private int replayCountdown;
  private String backupDirectory;

  private transient BufferedReader br;
  private transient FileSystem dstFs;
  
  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    if (backupDirectory != null) {
      try {
        dstFs = FileSystem.newInstance(new Path(backupDirectory).toUri(), configuration);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    if (dstFs != null) {
      try {
        dstFs.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    if (path.toString().endsWith(".gz")) {
      try {
        is = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, is);
        if (dstFs != null) {
          FileUtil.copy(fs, path, dstFs, new Path(backupDirectory, path.getName()), false, configuration);
        }
      }
      catch (CompressorException ce) {
        throw new IOException(ce);
      }
    }
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    br.close();
    super.closeFile(is);
  }

  @Override
  protected T readEntity() throws IOException
  {
    return readEntityFromReader(br);
  }
  
  public abstract T readEntityFromReader(BufferedReader reader);

  @Override
  protected void emit(T tuple)
  {
    output.emit(tuple);
  }

  public int getReplayCountdown()
  {
    return replayCountdown;
  }

  public void setReplayCountdown(int count)
  {
    this.replayCountdown = count;
  }

  public void setBackupDirectory(String backupDirectory)
  {
    this.backupDirectory = backupDirectory;
  }

  public String getBackupDirectory()
  {
    return this.backupDirectory;
  }
}
