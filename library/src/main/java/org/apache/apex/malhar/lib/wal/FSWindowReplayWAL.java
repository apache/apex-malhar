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
package org.apache.apex.malhar.lib.wal;

import java.io.IOException;
import java.util.TreeMap;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.TreeMultimap;

import com.datatorrent.netlet.util.Slice;

/**
 * A {@link FileSystemWAL} that WindowDataManager uses to save state of every window.
 *
 * @since 3.5.0
 */
public class FSWindowReplayWAL extends FileSystemWAL
{
  transient boolean readOnly;

  transient TreeMultimap<Integer, FileDescriptor> fileDescriptors = TreeMultimap.create();

  //all the readers will read to this point while replaying.
  transient FileSystemWALPointer walEndPointerAfterRecovery;
  transient Slice retrievedWindow;

  transient TreeMap<Long, Integer>  windowWalParts = new TreeMap<>();

  FSWindowReplayWAL()
  {
    super();
    setInBatchMode(true);
    setFileSystemWALWriter(new WriterThatFinalizesImmediately(this));
  }

  FSWindowReplayWAL(boolean readOnly)
  {
    this();
    this.readOnly = readOnly;
  }

  @Override
  public void setup()
  {
    try {
      if (getMaxLength() == 0) {
        setMaxLength(fileContext.getDefaultFileSystem().getServerDefaults().getBlockSize());
      }
      if (!readOnly) {
        getWriter().recover();
      }
    } catch (IOException e) {
      throw new RuntimeException("while setup");
    }
  }

  public FileSystemWALPointer getWalEndPointerAfterRecovery()
  {
    return walEndPointerAfterRecovery;
  }

  /**
   * Finalizes files just after rotation. Doesn't wait for the window to be committed.
   */
  static class WriterThatFinalizesImmediately extends FileSystemWAL.FileSystemWALWriter
  {

    private WriterThatFinalizesImmediately()
    {
      super();
    }

    protected WriterThatFinalizesImmediately(@NotNull FileSystemWAL fileSystemWal)
    {
      super(fileSystemWal);
    }

    /**
     * Finalize the file immediately after rotation.
     * @param partNum        part number
     * @throws IOException
     */
    @Override
    protected void rotated(int partNum) throws IOException
    {
      finalize(partNum);
    }

    @Override
    protected void recover() throws IOException
    {
      restoreActivePart();
    }
  }

  static class FileDescriptor implements Comparable<FileDescriptor>
  {
    int part;
    boolean isTmp;
    long time;
    Path filePath;

    static FileDescriptor create(Path filePath)
    {
      FileDescriptor descriptor = new FileDescriptor();
      descriptor.filePath = filePath;

      String name = filePath.getName();
      String[] parts = name.split("\\.");

      String[] namePart = parts[0].split("_");
      descriptor.part = Integer.parseInt(namePart[1]);

      if (parts.length == 3) {
        descriptor.isTmp = true;
        descriptor.time = Long.parseLong(parts[1]);
      }

      return descriptor;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileDescriptor)) {
        return false;
      }

      FileDescriptor that = (FileDescriptor)o;

      if (part != that.part) {
        return false;
      }
      if (isTmp != that.isTmp) {
        return false;
      }
      return time == that.time;

    }

    @Override
    public int hashCode()
    {
      int result = part;
      result = 31 * result + (isTmp ? 1 : 0);
      result = 31 * result + (int)(time ^ (time >>> 32));
      return result;
    }

    @Override
    public int compareTo(FileDescriptor o)
    {
      if (part < o.part) {
        return -1;
      } else if (part > o.part) {
        return 1;
      } else {
        if (isTmp && !o.isTmp) {
          return -1;
        } else if (!isTmp && o.isTmp) {
          return 1;
        }
        return Long.compare(time, o.time);
      }
    }
  }
}
