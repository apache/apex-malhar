/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import java.io.IOException;
import java.io.InputStream;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;

/**
 *
 */
/**
 * Input Adapter for reading from File<p>
 * <br>
 *
 * <br>
 *
 * @param <STREAM>
 * @since 0.3.2
 */
public abstract class AbstractFileInputOperator<STREAM extends InputStream> implements InputOperator, ActivationListener<OperatorContext>
{
  protected transient STREAM input;

  public String getFilePath()
  {
    return filePath;
  }

  /**
   * The file name. This can be a relative path for the default file system
   * or fully qualified URL as accepted by ({@link org.apache.hadoop.fs.Path}).
   * For splits with per file size limit, the name needs to
   * contain substitution tokens to generate unique file names.
   * Example: file:///mydir/adviews.out.%(operatorId).part-%(partIndex)
   *
   * @param filePath
   */
  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    input = openFile(filePath);
    seek(input, filepos);
  }

  @Override
  public void deactivate()
  {
    filepos = 0;
    try {
      input.close();
      input = null;
    }
    catch (IOException ex) {
      logger.error("exception on close", ex);
    }
  }

  @Override
  public void emitTuples()
  {
    emitTuples(input);
  }

  /**
   *
   * @param stream
   */
  public abstract void emitTuples(STREAM stream);

  @Override
  public void endWindow()
  {
    try {
      filepos = getFilePointer(input);
    }
    catch (RuntimeException re) {
      if (notWarned) {
        logger.warn("Exception while saving the file position, it makes the recovery impossible!", re.getCause());
        notWarned = false;
      }
    }
  }

  /**
   * Create an input stream for the path specified.
   *
   * @param filePath - path of the file which needs to be read.
   * @return - input stream created.
   */
  public abstract STREAM openFile(String filePath);

  /**
   * Returns the current offset in this file.
   *
   * @param stream
   * @return the offset from the beginning of the file, in bytes, at which the next read or write occurs.
   */
  public abstract long getFilePointer(STREAM stream);

  /**
   * Sets the file-pointer offset, measured from the beginning of this file, at which the next read or write occurs.
   * The offset may be set beyond the end of the file. Setting the offset beyond the end of the file does not change
   * the file length. The file length will change only by writing after the offset has been set beyond the end of the file.
   *
   * @param stream
   * @param pos the offset position, measured in bytes from the beginning of the file, at which to set the file pointer.
   */
  public abstract void seek(STREAM stream, long pos);

  @NotNull
  private String filePath;
  private long filepos;
  private transient boolean notWarned;
  private static final Logger logger = LoggerFactory.getLogger(AbstractFileInputOperator.class);
}
