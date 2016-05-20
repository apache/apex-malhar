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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.apex.malhar.lib.utils.IOUtils;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.local.LocalFs;
import org.apache.hadoop.fs.local.RawLocalFs;

import com.google.common.base.Preconditions;

import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.netlet.util.Slice;

/**
 * A WAL implementation that is file based.
 * <p/>
 * Note:<br/>
 * The FileSystem Writer and Reader operations should not alternate because intermingling these operations will cause
 * problems. Typically the  WAL Reader will only used in recovery.<br/>
 *
 * Also this implementation is thread unsafe- the filesystem wal writer and reader operations should be performed in
 * operator's thread.
 *
 * @since 3.4.0
 */
public class FileSystemWAL implements WAL<FileSystemWAL.FileSystemWALReader, FileSystemWAL.FileSystemWALWriter>
{

  @NotNull
  private String filePath;

  //max length of the file
  @Min(0)
  private long maxLength;

  private FileSystemWALPointer walStartPointer = new FileSystemWALPointer(0, 0);

  @NotNull
  private FileSystemWAL.FileSystemWALReader fileSystemWALReader = new FileSystemWALReader(this);

  @NotNull
  private FileSystemWAL.FileSystemWALWriter fileSystemWALWriter = new FileSystemWALWriter(this);

  //part => tmp file path;
  private final Map<Integer, String> tempPartFiles = new TreeMap<>();

  private long lastCheckpointedWindow = Stateless.WINDOW_ID;

  @Override
  public void setup()
  {
    try {
      FileContext fileContext = FileContextUtils.getFileContext(filePath);
      if (maxLength == 0) {
        maxLength = fileContext.getDefaultFileSystem().getServerDefaults().getBlockSize();
      }
      fileSystemWALWriter.open(fileContext);
      fileSystemWALReader.open(fileContext);
    } catch (IOException e) {
      throw new RuntimeException("during setup", e);
    }
  }

  @Override
  public void beforeCheckpoint(long window)
  {
    try {
      lastCheckpointedWindow = window;
      fileSystemWALWriter.flush();
    } catch (IOException e) {
      throw new RuntimeException("during before cp", e);
    }
  }

  /**
   * A temporary WAL file is not renamed as soon as it is closed (completed) instead it is tagged to be renamed with
   * respect to the window it gets closed. The actual renaming is deferred until the window gets committed.<br/>
   * <br/>
   * The reason to do so is because after a failure, when the WAL is restored to a checkpoint it might expect to write
   * to a temporary file. For example, the writer starts writing to WAL_10_x.tmp in window 59 and then gets
   * check-pointed.<br/>
   * In window 60 it completes writing to WAL_10_x.tmp and then closes it. If the file is renamed right then and there
   * is a failure then after restoration with the state of window 59, it will expect to continue writing to WAL_10_x.tmp
   * which will not be found.<br/>
   * <br/>
   * Writing to the final target (WAL part file) is avoided unless it is certain that the file will not be open to write
   * again and this happens when the window in which the file was completed gets committed.
   *
   * @param window committed window
   */
  @Override
  public void committed(long window)
  {
    try {
      fileSystemWALWriter.finalizeFiles(window);
    } catch (IOException e) {
      throw new RuntimeException("during committed", e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      fileSystemWALReader.close();
      fileSystemWALWriter.close();
    } catch (IOException e) {
      throw new RuntimeException("during teardown", e);
    }
  }

  protected long getLastCheckpointedWindow()
  {
    return lastCheckpointedWindow;
  }

  protected String getPartFilePath(int partNumber)
  {
    return filePath + "_" + partNumber;
  }

  @Override
  public FileSystemWALReader getReader()
  {
    return fileSystemWALReader;
  }

  /**
   * Sets the  File System WAL Reader. This can be used to override the default wal reader.
   *
   * @param fileSystemWALReader wal reader.
   */
  public void setFileSystemWALReader(@NotNull FileSystemWALReader fileSystemWALReader)
  {
    this.fileSystemWALReader = Preconditions.checkNotNull(fileSystemWALReader, "filesystem wal reader");
  }

  @Override
  public FileSystemWALWriter getWriter()
  {
    return fileSystemWALWriter;
  }

  /**
   * Sets the File System WAL Writer. This can be used to override the default wal writer.
   *
   * @param fileSystemWALWriter wal writer.
   */
  public void setFileSystemWALWriter(@NotNull FileSystemWALWriter fileSystemWALWriter)
  {
    this.fileSystemWALWriter = Preconditions.checkNotNull(fileSystemWALWriter, "filesystem wal writer");
  }

  /**
   * @return WAL file path
   */
  public String getFilePath()
  {
    return filePath;
  }

  /**
   * Sets the WAL file path.
   *
   * @param filePath WAL file path
   */
  public void setFilePath(@NotNull String filePath)
  {
    this.filePath = Preconditions.checkNotNull(filePath, "filePath");
  }

  /**
   * @return max length of a WAL part file.
   */
  public long getMaxLength()
  {
    return maxLength;
  }

  /**
   * Sets the maximum length of a WAL part file.
   *
   * @param maxLength max length of the WAL part file
   */
  public void setMaxLength(long maxLength)
  {
    this.maxLength = maxLength;
  }

  public static class FileSystemWALPointer implements Comparable<FileSystemWALPointer>
  {
    private final int partNum;
    private long offset;

    private FileSystemWALPointer()
    {
      //for kryo
      partNum = -1;
    }

    public FileSystemWALPointer(int partNum, long offset)
    {
      this.partNum = partNum;
      this.offset = offset;
    }

    @Override
    public int compareTo(@NotNull FileSystemWALPointer o)
    {
      int partNumComparison = Integer.compare(this.partNum, o.partNum);
      return partNumComparison == 0 ? Long.compare(this.offset, o.offset) : partNumComparison;
    }

    public int getPartNum()
    {
      return partNum;
    }

    public long getOffset()
    {
      return offset;
    }

    @Override
    public String toString()
    {
      return "FileSystemWalPointer{" + "partNum=" + partNum + ", offset=" + offset + '}';
    }
  }

  /**
   * A FileSystem Wal Reader
   */
  public static class FileSystemWALReader implements WAL.WALReader<FileSystemWALPointer>
  {
    private FileSystemWALPointer currentPointer;

    private transient DataInputStream inputStream;
    private transient Path currentOpenPath;
    private transient boolean isOpenPathTmp;

    private final FileSystemWAL fileSystemWAL;
    private transient FileContext fileContext;

    private FileSystemWALReader()
    {
      //for kryo
      fileSystemWAL = null;
    }

    public FileSystemWALReader(@NotNull FileSystemWAL fileSystemWal)
    {
      this.fileSystemWAL = Preconditions.checkNotNull(fileSystemWal, "wal");
      currentPointer = new FileSystemWALPointer(fileSystemWal.walStartPointer.partNum,
          fileSystemWal.walStartPointer.offset);
    }

    protected void open(@NotNull FileContext fileContext) throws IOException
    {
      this.fileContext = Preconditions.checkNotNull(fileContext, "fileContext");
    }

    protected void close() throws IOException
    {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    }

    @Override
    public void seek(FileSystemWALPointer pointer) throws IOException
    {
      Preconditions.checkArgument(pointer.compareTo(fileSystemWAL.walStartPointer) >= 0, "invalid pointer");
      if (inputStream != null) {
        close();
      }
      inputStream = getInputStream(pointer);
      currentPointer = pointer;
    }

    /**
     * Move to the next WAL segment.
     *
     * @return true if the next part file exists and is opened; false otherwise.
     * @throws IOException
     */
    private boolean nextSegment() throws IOException
    {
      if (inputStream != null) {
        close();
      }

      currentPointer = new FileSystemWALPointer(currentPointer.partNum + 1, 0);
      inputStream = getInputStream(currentPointer);

      return inputStream != null;
    }

    private DataInputStream getInputStream(FileSystemWALPointer walPointer) throws IOException
    {
      Preconditions.checkArgument(inputStream == null, "input stream not null");
      Path pathToReadFrom;
      String tmpPath = fileSystemWAL.tempPartFiles.get(walPointer.getPartNum());
      if (tmpPath != null) {
        pathToReadFrom = new Path(tmpPath);
        isOpenPathTmp = true;
      } else {
        pathToReadFrom = new Path(fileSystemWAL.getPartFilePath(walPointer.partNum));
        isOpenPathTmp = false;
      }

      LOG.debug("path to read {} and pointer {}", pathToReadFrom, walPointer);
      if (fileContext.util().exists(pathToReadFrom)) {
        DataInputStream stream = fileContext.open(pathToReadFrom);
        if (walPointer.offset > 0) {
          stream.skip(walPointer.offset);
        }
        currentOpenPath = pathToReadFrom;
        return stream;
      }
      return null;
    }

    @Override
    public Slice next() throws IOException
    {
      do {
        if (inputStream == null) {
          inputStream = getInputStream(currentPointer);
        }

        if (inputStream != null && isOpenPathTmp  && !fileSystemWAL.tempPartFiles.containsKey(currentPointer.partNum)) {
          //if the tmp path was finalized the path may not exist any more
          close();
          inputStream = getInputStream(currentPointer);
        }

        if (inputStream != null && currentPointer.offset < fileContext.getFileStatus(currentOpenPath).getLen()) {
          int len = inputStream.readInt();
          Preconditions.checkState(len >= 0, "negative length");

          byte[] data = new byte[len];
          inputStream.readFully(data);

          currentPointer.offset += data.length + 4;
          return new Slice(data);
        }
      } while (nextSegment());
      close();
      return null;
    }

    @Override
    public FileSystemWALPointer getStartPointer()
    {
      return fileSystemWAL.walStartPointer;
    }
  }

  /**
   * A FileSystem WAL Writer.
   */
  public static class FileSystemWALWriter implements WAL.WALWriter<FileSystemWALPointer>
  {
    private FileSystemWALPointer currentPointer = new FileSystemWALPointer(0, 0);
    private transient DataOutputStream outputStream;

    //windowId => Latest part which can be finalized.
    private final Map<Long, Integer> pendingFinalization = new TreeMap<>();

    private final FileSystemWAL fileSystemWAL;
    private transient FileContext fileContext;

    private int latestFinalizedPart = -1;
    private int lowestDeletedPart = -1;

    private FileSystemWALWriter()
    {
      //for kryo
      fileSystemWAL = null;
    }

    public FileSystemWALWriter(@NotNull FileSystemWAL fileSystemWal)
    {
      this.fileSystemWAL = Preconditions.checkNotNull(fileSystemWal, "wal");
    }

    protected void open(@NotNull FileContext fileContext) throws IOException
    {
      this.fileContext = Preconditions.checkNotNull(fileContext, "file context");
      recover();
    }

    private void recover() throws IOException
    {
      LOG.debug("current point", currentPointer);
      String tmpFilePath = fileSystemWAL.tempPartFiles.get(currentPointer.getPartNum());
      if (tmpFilePath != null) {

        Path tmpPath = new Path(tmpFilePath);
        if (fileContext.util().exists(tmpPath)) {
          LOG.debug("tmp path exists {}", tmpPath);

          outputStream = getOutputStream(new FileSystemWALPointer(currentPointer.partNum, 0));
          DataInputStream inputStreamOldTmp = fileContext.open(tmpPath);

          IOUtils.copyPartial(inputStreamOldTmp, currentPointer.offset, outputStream);

          outputStream.flush();
          //remove old tmp
          inputStreamOldTmp.close();
          LOG.debug("delete tmp {}", tmpPath);
          fileContext.delete(tmpPath, true);
        }
      }

      //find all valid path names
      Set<String> validPathNames = new HashSet<>();
      for (Map.Entry<Integer, String> entry : fileSystemWAL.tempPartFiles.entrySet()) {
        if (entry.getKey() <= currentPointer.partNum) {
          validPathNames.add(new Path(entry.getValue()).getName());
        }
      }
      LOG.debug("valid names {}", validPathNames);

      //there can be a failure just between the flush and the actual checkpoint which can leave some stray tmp files
      //which aren't accounted by tmp files map
      Path walPath = new Path(fileSystemWAL.filePath);
      Path parentWAL = walPath.getParent();
      if (parentWAL != null && fileContext.util().exists(parentWAL)) {
        RemoteIterator<FileStatus> remoteIterator = fileContext.listStatus(parentWAL);
        while (remoteIterator.hasNext()) {
          FileStatus status = remoteIterator.next();
          String fileName = status.getPath().getName();
          if (fileName.startsWith(walPath.getName()) && fileName.endsWith(TMP_EXTENSION) &&
              !validPathNames.contains(fileName)) {
            LOG.debug("delete stray tmp {}", status.getPath());
            fileContext.delete(status.getPath(), true);
          }
        }
      }

    }

    protected void close() throws IOException
    {
      if (outputStream != null) {
        outputStream.close();
        outputStream = null;
        LOG.debug("closed {}", currentPointer.partNum);
      }
    }

    @Override
    public int append(Slice entry) throws IOException
    {
      if (outputStream == null) {
        outputStream = getOutputStream(currentPointer);
      }

      int entryLength = entry.length + 4;

      // rotate if needed
      if (shouldRotate(entryLength)) {
        rotate(true);
      }

      outputStream.writeInt(entry.length);
      outputStream.write(entry.buffer, entry.offset, entry.length);
      currentPointer.offset += entryLength;

      if (currentPointer.offset == fileSystemWAL.maxLength) {
        //if the file is completed then we can rotate it. do not have to wait for next entry
        rotate(false);
      }

      return entryLength;
    }

    @Override
    public FileSystemWALPointer getPointer()
    {
      return currentPointer;
    }

    @Override
    public void delete(FileSystemWALPointer pointer) throws IOException
    {
      if (pointer.compareTo(currentPointer) <= 0) {
        fileSystemWAL.walStartPointer = pointer;
        deleteFinalizedParts(pointer);
      }
    }

    private void deleteFinalizedParts(FileSystemWALPointer pointer) throws IOException
    {

      int lastPartDeleted = -1;

      //delete all part files completely which are smaller than pointer.partNum
      for (int i = lowestDeletedPart + 1; i < pointer.partNum; i++) {
        if (i <= latestFinalizedPart) {
          //delete a part only if it is finalized.
          Path partPath = new Path(fileSystemWAL.getPartFilePath(i));
          if (fileContext.util().exists(partPath)) {
            LOG.debug("delete {}", partPath);
            fileContext.delete(partPath, true);
            lastPartDeleted = i;
          } else {
            break;
          }
        }
      }
      if (lastPartDeleted != -1) {
        lowestDeletedPart = lastPartDeleted;
      }

      //truncate the pointer.partNum till pointer.offset
      if (pointer.partNum <= latestFinalizedPart && pointer.offset > 0) {
        String part = fileSystemWAL.getPartFilePath(pointer.partNum);
        Path inputPartPath = new Path(part);
        long length = fileContext.getFileStatus(inputPartPath).getLen();

        LOG.debug("truncate {} from {} length {}", part, pointer.offset, length);

        if (length > pointer.offset) {

          String temp = getTmpFilePath(part);
          Path tmpPart = new Path(temp);

          DataInputStream inputStream = fileContext.open(inputPartPath);
          DataOutputStream outputStream = fileContext.create(tmpPart, EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND),
              Options.CreateOpts.CreateParent.createParent());

          IOUtils.copyPartial(inputStream, pointer.offset, length - pointer.offset, outputStream);
          inputStream.close();
          outputStream.close();

          if (fileSystemWAL.walStartPointer.partNum == pointer.partNum) {
            //Since the file is abridged
            fileSystemWAL.walStartPointer.offset = 0;
          }

          fileContext.rename(tmpPart, inputPartPath, Options.Rename.OVERWRITE);
        }
      }
    }

    protected void flush() throws IOException
    {
      if (outputStream != null) {
        if (fileContext.getDefaultFileSystem() instanceof LocalFs ||
            fileContext.getDefaultFileSystem() instanceof RawLocalFs) {
          //until the stream is closed on the local FS, readers don't see any data.
          close();
        } else {
          Syncable syncableOutputStream = (Syncable)outputStream;
          syncableOutputStream.hflush();
          syncableOutputStream.hsync();
        }
      }
    }

    protected boolean shouldRotate(int entryLength)
    {
      return currentPointer.offset + entryLength > fileSystemWAL.maxLength;
    }

    protected void rotate(boolean openNextFile) throws IOException
    {
      flush();
      close();
      //all parts up to current part can be finalized.
      pendingFinalization.put(fileSystemWAL.getLastCheckpointedWindow(), currentPointer.partNum);

      LOG.debug("rotate {} to {}", currentPointer.partNum, currentPointer.partNum + 1);
      currentPointer = new FileSystemWALPointer(currentPointer.partNum + 1, 0);
      if (openNextFile) {
        //if adding the new entry to the file can cause the current file to exceed the max length then it is rotated.
        outputStream = getOutputStream(currentPointer);
      }
    }

    protected void finalizeFiles(long window) throws IOException
    {
      if (!fileSystemWAL.tempPartFiles.isEmpty()) {
        //finalize temporary files
        int largestPartAvailable = fileSystemWAL.tempPartFiles.keySet().iterator().next();

        Iterator<Map.Entry<Long, Integer>> pendingFinalizeIter = pendingFinalization.entrySet().iterator();
        while (pendingFinalizeIter.hasNext()) {
          Map.Entry<Long, Integer> entry = pendingFinalizeIter.next();

          if (entry.getKey() >= window) {
            //finalize files which were requested for finalization in the window < committed window
            break;
          }
          pendingFinalizeIter.remove();

          int partToFinalizeTill = entry.getValue();
          for (int i = largestPartAvailable; i <= partToFinalizeTill; i++) {
            String tmpToFinalize = fileSystemWAL.tempPartFiles.remove(i);
            Path tmpPath = new Path(tmpToFinalize);

            if (fileContext.util().exists(tmpPath)) {
              LOG.debug("finalize {} of part {}", tmpToFinalize, i);
              fileContext.rename(tmpPath, new Path(fileSystemWAL.getPartFilePath(i)), Options.Rename.OVERWRITE);
              latestFinalizedPart = i;
            }
          }
          largestPartAvailable = partToFinalizeTill + 1;
        }
      }

      if (lowestDeletedPart != -1 && lowestDeletedPart < fileSystemWAL.walStartPointer.partNum) {
        //delete any pending finalized files which were not deleted when the delete request was made.
        deleteFinalizedParts(fileSystemWAL.walStartPointer);
      }
    }

    private DataOutputStream getOutputStream(FileSystemWALPointer pointer) throws IOException
    {
      Preconditions.checkArgument(outputStream == null, "output stream is not null");

      if (pointer.offset > 0 && (fileContext.getDefaultFileSystem() instanceof LocalFs ||
          fileContext.getDefaultFileSystem() instanceof RawLocalFs)) {
        //on local file system the stream is closed instead of flush so we open it again in append mode if the
        //offset > 0.
        return fileContext.create(new Path(fileSystemWAL.tempPartFiles.get(pointer.partNum)),
            EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND), Options.CreateOpts.CreateParent.createParent());
      }

      String partFile = fileSystemWAL.getPartFilePath(pointer.partNum);
      String tmpFilePath = getTmpFilePath(partFile);
      fileSystemWAL.tempPartFiles.put(pointer.partNum, tmpFilePath);

      Preconditions.checkArgument(pointer.offset == 0, "offset > 0");
      LOG.debug("open {} => {}", pointer.partNum, tmpFilePath);
      outputStream = fileContext.create(new Path(tmpFilePath),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.CreateParent.createParent());
      return outputStream;
    }

  }

  private static String getTmpFilePath(String filePath)
  {
    return filePath + '.' + System.currentTimeMillis() + TMP_EXTENSION;
  }

  private static final String TMP_EXTENSION = ".tmp";

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWAL.class);
}
