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
import java.io.EOFException;
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
 * problems. Typically the WAL Reader will only used in recovery or replay of finished windows.<br/>
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

  FileSystemWALPointer walStartPointer = new FileSystemWALPointer(0, 0);

  @NotNull
  private FileSystemWAL.FileSystemWALReader fileSystemWALReader = new FileSystemWALReader(this);

  @NotNull
  private FileSystemWAL.FileSystemWALWriter fileSystemWALWriter = new FileSystemWALWriter(this);

  //part => tmp file filePath;
  final Map<Integer, String> tempPartFiles = new TreeMap<>();

  private long lastCheckpointedWindow = Stateless.WINDOW_ID;

  private boolean hardLimitOnMaxLength;

  private boolean inBatchMode;

  transient FileContext fileContext;

  @Override
  public void setup()
  {
    try {
      fileContext = FileContextUtils.getFileContext(filePath);
      if (maxLength == 0) {
        maxLength = fileContext.getDefaultFileSystem().getServerDefaults().getBlockSize();
      }
      fileSystemWALWriter.recover();
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

  /**
   * @return the wal start pointer
   */
  public FileSystemWALPointer getWalStartPointer()
  {
    return walStartPointer;
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

  /**
   * @return true if there is a hard limit on max length; false otherwise.
   */
  public boolean isHardLimitOnMaxLength()
  {
    return hardLimitOnMaxLength;
  }

  /**
   * When hard limit on max length is true, then a wal part file will never exceed the the max length.<br/>
   * Entry is appended to the next part if adding it to the current part exceeds the max length.
   * <p/>
   * When hard limit on max length if false, then a wal part file can exceed the max length if the last entry makes the
   * wal part exceeds the max length. By default this is set to false.
   *
   * @param hardLimitOnMaxLength
   */
  public void setHardLimitOnMaxLength(boolean hardLimitOnMaxLength)
  {
    this.hardLimitOnMaxLength = hardLimitOnMaxLength;
  }

  /**
   * @return true if writing in batch mode; false otherwise.
   */
  protected boolean isInBatchMode()
  {
    return inBatchMode;
  }

  /**
   * When in batch mode, a file is rotated only when a batch gets completed. This facilitates writing multiple entries
   * that will all be written to the same part file.
   *
   * @param inBatchMode write in batch mode or not.
   */
  protected void setInBatchMode(boolean inBatchMode)
  {
    this.inBatchMode = inBatchMode;
  }

  public static class FileSystemWALPointer implements Comparable<FileSystemWALPointer>
  {
    private final int partNum;
    private long offset;

    @SuppressWarnings("unused")
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

    public FileSystemWALPointer getCopy()
    {
      return new FileSystemWALPointer(partNum, offset);
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
    private transient FileSystemWALPointer currentPointer;

    private transient DataInputStream inputStream;
    private transient boolean isOpenPathTmp;

    private final FileSystemWAL fileSystemWAL;

    protected FileSystemWALReader()
    {
      //for kryo
      fileSystemWAL = null;
    }

    public FileSystemWALReader(@NotNull FileSystemWAL fileSystemWal)
    {
      this.fileSystemWAL = Preconditions.checkNotNull(fileSystemWal, "wal");
    }

    @Override
    public void close() throws IOException
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

      if (fileSystemWAL.fileContext.util().exists(pathToReadFrom)) {
        LOG.debug("filePath to read {} and pointer {}", pathToReadFrom, walPointer);
        DataInputStream stream = fileSystemWAL.fileContext.open(pathToReadFrom);
        if (walPointer.offset > 0) {
          stream.skip(walPointer.offset);
        }
        return stream;
      }
      return null;
    }

    @Override
    public Slice next() throws IOException
    {
      return readOrSkip(false);
    }

    @Override
    public void skipNext() throws IOException
    {
      readOrSkip(true);
    }

    private static int readLen(final DataInputStream inputStream) throws IOException
    {
      if (inputStream == null) {
        return -1;
      }

      int len = inputStream.read();

      if (len < 0) {
        return len;
      }

      len = len << 24;

      for (int i = 2;i >= 0;--i) {
        int ch = inputStream.read();
        if (ch < 0) {
          throw new EOFException();
        }

        len += (ch << 8 * i);
      }

      if (len < 0) {
        throw new IOException("Negative length");
      }

      return len;
    }

    private Slice readOrSkip(boolean skip) throws IOException
    {
      if (currentPointer == null) {
        currentPointer = fileSystemWAL.walStartPointer;
      }
      do {
        if (inputStream == null) {
          inputStream = getInputStream(currentPointer);
        }

        if (inputStream != null && isOpenPathTmp  && !fileSystemWAL.tempPartFiles.containsKey(currentPointer.partNum)) {
          //if the tmp path was finalized the path may not exist any more
          close();
          inputStream = getInputStream(currentPointer);
        }

        int len = readLen(inputStream);

        if (len != -1) {
          if (!skip) {
            byte[] data = new byte[len];
            inputStream.readFully(data);
            currentPointer.offset += len + 4;
            return new Slice(data);

          } else {
            long actualSkipped = inputStream.skip(len);
            if (actualSkipped != len) {
              throw new IOException("unable to skip " + len);
            }
            currentPointer.offset += len + 4;
            return null;
          }
        }
      } while (nextSegment());
      close();
      return null;
    }

    public FileSystemWALPointer getCurrentPointer()
    {
      return currentPointer;
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

    private int latestFinalizedPart = -1;
    private int lowestDeletedPart = -1;

    protected FileSystemWALWriter()
    {
      //for kryo
      fileSystemWAL = null;
    }

    protected FileSystemWALWriter(@NotNull FileSystemWAL fileSystemWal)
    {
      this.fileSystemWAL = Preconditions.checkNotNull(fileSystemWal, "wal");
    }

    protected void recover() throws IOException
    {
      restoreActivePart();
      deleteStrayTmpFiles();
    }

    void restoreActivePart() throws IOException
    {
      LOG.debug("restore part {}", currentPointer);
      String tmpFilePath = fileSystemWAL.tempPartFiles.get(currentPointer.getPartNum());
      if (tmpFilePath != null) {

        Path inputPath = new Path(tmpFilePath);
        if (fileSystemWAL.fileContext.util().exists(inputPath)) {
          LOG.debug("input path exists {}", inputPath);

          //temp file output stream
          outputStream = getOutputStream(new FileSystemWALPointer(currentPointer.partNum, 0));
          DataInputStream inputStream = fileSystemWAL.fileContext.open(inputPath);

          IOUtils.copyPartial(inputStream, currentPointer.offset, outputStream);

          flush();
          inputStream.close();
        }
      }
    }

    void deleteStrayTmpFiles() throws IOException
    {
      //find all valid filePath names
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
      if (parentWAL != null && fileSystemWAL.fileContext.util().exists(parentWAL)) {
        RemoteIterator<FileStatus> remoteIterator = fileSystemWAL.fileContext.listStatus(parentWAL);
        while (remoteIterator.hasNext()) {
          FileStatus status = remoteIterator.next();
          String fileName = status.getPath().getName();
          if (fileName.startsWith(walPath.getName()) && fileName.endsWith(TMP_EXTENSION) &&
              !validPathNames.contains(fileName)) {
            LOG.debug("delete stray tmp {}", status.getPath());
            fileSystemWAL.fileContext.delete(status.getPath(), true);
          }
        }
      }
    }

    @Override
    public void close() throws IOException
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

      int entrySize = entry.length + 4;

      if (fileSystemWAL.hardLimitOnMaxLength) {
        Preconditions.checkArgument(entrySize > fileSystemWAL.maxLength, "entry too big. increase the max length");
      }

      // rotate if needed
      if (fileSystemWAL.hardLimitOnMaxLength && shouldRotate(entrySize) && !fileSystemWAL.inBatchMode) {
        rotate(true);
      }

      outputStream.writeInt(entry.length);
      outputStream.write(entry.buffer, entry.offset, entry.length);
      currentPointer.offset += entrySize;

      if (currentPointer.offset >= fileSystemWAL.maxLength && !fileSystemWAL.inBatchMode) {
        //if the file is completed then we can rotate it. do not have to wait for next entry
        rotate(false);
      }

      return entrySize;
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
          if (fileSystemWAL.fileContext.util().exists(partPath)) {
            LOG.debug("delete {}", partPath);
            fileSystemWAL.fileContext.delete(partPath, true);
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
        long length = fileSystemWAL.fileContext.getFileStatus(inputPartPath).getLen();

        LOG.debug("truncate {} from {} length {}", part, pointer.offset, length);

        if (length > pointer.offset) {

          String temp = createTmpFilePath(part);
          Path tmpPart = new Path(temp);

          DataInputStream inputStream = fileSystemWAL.fileContext.open(inputPartPath);
          DataOutputStream outputStream = fileSystemWAL.fileContext.create(tmpPart,
              EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.CreateParent.createParent());

          IOUtils.copyPartial(inputStream, pointer.offset, length - pointer.offset, outputStream);
          inputStream.close();
          outputStream.close();

          if (fileSystemWAL.walStartPointer.partNum == pointer.partNum) {
            //Since the file is abridged
            fileSystemWAL.walStartPointer.offset = 0;
          }

          fileSystemWAL.fileContext.rename(tmpPart, inputPartPath, Options.Rename.OVERWRITE);
        }
      }
    }

    protected void flush() throws IOException
    {
      if (outputStream != null) {
        if (fileSystemWAL.fileContext.getDefaultFileSystem() instanceof LocalFs ||
            fileSystemWAL.fileContext.getDefaultFileSystem() instanceof RawLocalFs) {
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

    void rotate(boolean openNextFile) throws IOException
    {
      flush();
      close();

      int partNum = currentPointer.partNum;
      LOG.debug("rotate {} to {}", partNum, currentPointer.partNum + 1);
      currentPointer = new FileSystemWALPointer(currentPointer.partNum + 1, 0);
      if (openNextFile) {
        //if adding the new entry to the file can cause the current file to exceed the max length then it is rotated.
        outputStream = getOutputStream(currentPointer);
      }

      rotated(partNum);
    }

    /**
     * When the wal is used in batch-mode, this method will trigger rotation if the current part file is completed.
     * @throws IOException
     */
    protected void rotateIfNecessary() throws IOException
    {
      if (fileSystemWAL.inBatchMode && currentPointer.offset >= fileSystemWAL.maxLength) {
        //if the file is completed then we can rotate it
        rotate(false);
      }
    }

    protected void rotated(int partNum) throws IOException
    {
      //all parts up to current part can be finalized.
      pendingFinalization.put(fileSystemWAL.getLastCheckpointedWindow(), partNum);
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
            finalize(i);
          }
          largestPartAvailable = partToFinalizeTill + 1;
        }
      }

      if (lowestDeletedPart != -1 && lowestDeletedPart < fileSystemWAL.walStartPointer.partNum) {
        //delete any pending finalized files which were not deleted when the delete request was made.
        deleteFinalizedParts(fileSystemWAL.walStartPointer);
      }
    }

    protected void finalize(int partNum) throws IOException
    {
      String tmpToFinalize = fileSystemWAL.tempPartFiles.remove(partNum);
      Path tmpPath = new Path(tmpToFinalize);
      if (fileSystemWAL.fileContext.util().exists(tmpPath)) {
        LOG.debug("finalize {} of part {}", tmpPath, partNum);
        fileSystemWAL.fileContext.rename(tmpPath, new Path(fileSystemWAL.getPartFilePath(partNum)),
            Options.Rename.OVERWRITE);
        latestFinalizedPart = partNum;
      }
    }

    private DataOutputStream getOutputStream(FileSystemWALPointer pointer) throws IOException
    {
      Preconditions.checkArgument(outputStream == null, "output stream is not null");

      if (pointer.offset > 0 && (fileSystemWAL.fileContext.getDefaultFileSystem() instanceof LocalFs ||
          fileSystemWAL.fileContext.getDefaultFileSystem() instanceof RawLocalFs)) {
        //On local file system the stream is always closed and never flushed so we open it again in append mode if the
        //offset > 0. This block is entered only when appending to wal while writing on local fs.
        return fileSystemWAL.fileContext.create(new Path(fileSystemWAL.tempPartFiles.get(pointer.partNum)),
            EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND), Options.CreateOpts.CreateParent.createParent());
      }

      String partFile = fileSystemWAL.getPartFilePath(pointer.partNum);
      String tmpFilePath = createTmpFilePath(partFile);
      fileSystemWAL.tempPartFiles.put(pointer.partNum, tmpFilePath);

      Preconditions.checkArgument(pointer.offset == 0, "offset > 0");
      LOG.debug("open {} => {}", pointer.partNum, tmpFilePath);
      outputStream = fileSystemWAL.fileContext.create(new Path(tmpFilePath),
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), Options.CreateOpts.CreateParent.createParent());
      return outputStream;
    }

    //visible to WindowDataManager
    FileSystemWALPointer getCurrentPointer()
    {
      return currentPointer;
    }

    //visible to WindowDataManager
    void setCurrentPointer(@NotNull FileSystemWALPointer pointer)
    {
      this.currentPointer = Preconditions.checkNotNull(pointer, "pointer");
    }
  }

  private static String createTmpFilePath(String filePath)
  {
    return filePath + '.' + System.currentTimeMillis() + TMP_EXTENSION;
  }

  static final String TMP_EXTENSION = ".tmp";

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemWAL.class);
}
