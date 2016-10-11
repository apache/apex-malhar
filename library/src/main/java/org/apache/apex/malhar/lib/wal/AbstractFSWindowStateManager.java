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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.managed.IncrementalCheckpointManagerImpl;
import org.apache.apex.malhar.lib.utils.FileContextUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.Slice;

public abstract class AbstractFSWindowStateManager implements Component<Context.OperatorContext>
{

  private static final String WAL_FILE_NAME = "wal";

  //operator id -> wals (sorted)
  protected final transient Map<Integer, FSWindowReplayWAL> readOnlyWals = new HashMap<>();
  protected final FSWindowReplayWAL wal = new FSWindowReplayWAL();

  /**
   * This is not null only for one physical instance.<br/>
   * It consists of operator ids which have been deleted but have some state that can be replayed.
   * Only one of the instances would be handling (modifying) the files that belong to this state. <br/>
   * The value is assigned during partitioning.
   */
  protected Set<Integer> deletedOperators;
  protected boolean repartitioned;

  /**
   * Used when it is not necessary to replay every streaming/app window.
   * Used by {@link IncrementalCheckpointManagerImpl}
   */
  protected boolean relyOnCheckpoints;

  protected transient long largestCompletedWindow = Stateless.WINDOW_ID;
  protected transient String fullStatePath;
  protected transient int operatorId;
  protected transient FileContext fileContext;

  private final transient Kryo kryo = new Kryo();

  /**
   * State path relative to app filePath where state is saved.
   */
  @NotNull
  private String statePath;
  private boolean isStatePathRelativeToAppPath = true;

  private transient boolean areWALReadersClosed;

  protected AbstractFSWindowStateManager()
  {
    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    operatorId = context.getId();

    if (isStatePathRelativeToAppPath) {
      fullStatePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + statePath;
    } else {
      fullStatePath = statePath;
    }

    try {
      fileContext = FileContextUtils.getFileContext(fullStatePath);
      setupWals(context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupWals(long activationWindow) throws IOException
  {
    findFiles(wal, operatorId);
    configureWal(wal, operatorId, !relyOnCheckpoints);

    if (repartitioned) {
      createReadOnlyWals();
      for (Map.Entry<Integer, FSWindowReplayWAL> entry : readOnlyWals.entrySet()) {
        findFiles(entry.getValue(), entry.getKey());
        configureWal(entry.getValue(), entry.getKey(), true);
      }
    }

    //find largest completed window
    if (!relyOnCheckpoints) {
      long completedWindow = findLargestCompletedWindow(wal, null);
      //committed will not delete temp files so it is possible that when reading from files, a smaller window
      //than the activation window is found.
      if (completedWindow > activationWindow) {
        largestCompletedWindow = completedWindow;
      }
      if (wal.getReader().getCurrentPointer() != null) {
        wal.getWriter().setCurrentPointer(wal.getReader().getCurrentPointer().getCopy());
      }
    } else {
      wal.walEndPointerAfterRecovery = wal.getWriter().getCurrentPointer();
      largestCompletedWindow = wal.getLastCheckpointedWindow();
    }

    if (repartitioned && largestCompletedWindow > Stateless.WINDOW_ID) {
      //find the min of max window ids: a downstream will not finish a window until all the upstream have finished it.
      for (Map.Entry<Integer, FSWindowReplayWAL> entry : readOnlyWals.entrySet()) {

        long completedWindow = Stateless.WINDOW_ID;
        if (!relyOnCheckpoints) {
          long window = findLargestCompletedWindow(entry.getValue(), null);
          if (window > activationWindow) {
            completedWindow = window;
          }
        } else {
          completedWindow = findLargestCompletedWindow(entry.getValue(), activationWindow);
        }

        if (completedWindow < largestCompletedWindow) {
          largestCompletedWindow = completedWindow;
        }
      }
    }

    //reset readers
    wal.getReader().seek(wal.walStartPointer);
    for (FSWindowReplayWAL wal : readOnlyWals.values()) {
      wal.getReader().seek(wal.walStartPointer);
    }

    wal.setup();
    for (FSWindowReplayWAL wal : readOnlyWals.values()) {
      wal.setup();
    }
  }

  protected void createReadOnlyWals() throws IOException
  {
    RemoteIterator<FileStatus> operatorsIter = fileContext.listStatus(new Path(fullStatePath));
    while (operatorsIter.hasNext()) {
      FileStatus status = operatorsIter.next();
      int operatorId = Integer.parseInt(status.getPath().getName());

      if (operatorId != this.operatorId) {
        //create read-only wal for other partitions
        FSWindowReplayWAL wal = new FSWindowReplayWAL(true);
        readOnlyWals.put(operatorId, wal);
      }
    }
  }

  private void configureWal(FSWindowReplayWAL wal, int operatorId, boolean updateWalState) throws IOException
  {
    String operatorDir = fullStatePath + Path.SEPARATOR + operatorId;
    wal.setFilePath(operatorDir + Path.SEPARATOR + WAL_FILE_NAME);
    wal.fileContext = fileContext;

    if (updateWalState) {
      if (!wal.fileDescriptors.isEmpty()) {
        SortedSet<Integer> sortedParts = wal.fileDescriptors.keySet();

        wal.walStartPointer = new FileSystemWAL.FileSystemWALPointer(sortedParts.first(), 0);

        FSWindowReplayWAL.FileDescriptor last = wal.fileDescriptors.get(sortedParts.last()).last();
        if (last.isTmp()) {
          wal.tempPartFiles.put(last.getPart(), last.getFilePath().toString());
        }
      }
    }
  }

  private void findFiles(FSWindowReplayWAL wal, int operatorId) throws IOException
  {
    String operatorDir = fullStatePath + Path.SEPARATOR + operatorId;
    Path operatorPath = new Path(operatorDir);
    if (fileContext.util().exists(operatorPath)) {
      RemoteIterator<FileStatus> walFilesIter = fileContext.listStatus(operatorPath);

      while (walFilesIter.hasNext()) {
        FileStatus fileStatus = walFilesIter.next();
        FSWindowReplayWAL.FileDescriptor descriptor = FSWindowReplayWAL.FileDescriptor.create(fileStatus.getPath());
        wal.fileDescriptors.put(descriptor.getPart(), descriptor);
      }
    }
  }

  private long findLargestCompletedWindow(FSWindowReplayWAL wal, Long ceilingWindow) throws IOException
  {
    if (!wal.fileDescriptors.isEmpty()) {
      FileSystemWAL.FileSystemWALReader reader = wal.getReader();

      //to find the largest window, we only need to read part files in descending order
      NavigableSet<Integer> descendingParts = new TreeSet<>(wal.fileDescriptors.keySet()).descendingSet();
      for (int part : descendingParts) {
        FSWindowReplayWAL.FileDescriptor last = wal.fileDescriptors.get(part).last();
        reader.seek(new FileSystemWAL.FileSystemWALPointer(last.getPart(), 0));

        long endOffset = -1;

        long lastWindow = Stateless.WINDOW_ID;
        Slice slice = readNext(reader);

        while (slice != null) {
          boolean skipComplete = skipNext(reader); //skip the artifact because we need just the largest window id.
          if (!skipComplete) {
            //artifact not saved so this window was not finished.
            break;
          }
          long offset = reader.getCurrentPointer().getOffset();

          long window = Longs.fromByteArray(slice.toByteArray());
          if (ceilingWindow != null && window > ceilingWindow) {
            break;
          }
          endOffset = offset;
          lastWindow = window;
          slice = readNext(reader);  //either null or next window
        }

        if (endOffset != -1) {
          wal.walEndPointerAfterRecovery = new FileSystemWAL.FileSystemWALPointer(last.getPart(), endOffset);
          wal.windowWalParts.put(lastWindow, wal.walEndPointerAfterRecovery.getPartNum());
          return lastWindow;
        }
      }
    }
    return Stateless.WINDOW_ID;
  }

  protected Slice toSlice(Object object)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClassAndObject(output, object);
    output.close();
    byte[] bytes = baos.toByteArray();

    return new Slice(bytes);
  }

  protected Object fromSlice(Slice slice)
  {
    Input input = new Input(slice.buffer, slice.offset, slice.length);
    Object object = kryo.readClassAndObject(input);
    input.close();
    return object;
  }

  protected void closeReadersIfNecessary() throws IOException
  {
    if (areWALReadersClosed) {
      return;
    }
    areWALReadersClosed = true;
    //close all reader stream and remove read-only wals
    wal.getReader().close();
    if (readOnlyWals.size() > 0) {
      Iterator<Map.Entry<Integer, FSWindowReplayWAL>> walIterator = readOnlyWals.entrySet().iterator();
      while (walIterator.hasNext()) {
        Map.Entry<Integer, FSWindowReplayWAL> entry = walIterator.next();
        entry.getValue().getReader().close();

        int operatorId = entry.getKey();
        if (deletedOperators == null || !deletedOperators.contains(operatorId)) {
          //the read only wal can be removed.
          walIterator.remove();
        }
      }
    }
  }

  @Override
  public void teardown()
  {
    wal.teardown();
    for (FSWindowReplayWAL wal : readOnlyWals.values()) {
      wal.teardown();
    }
  }

  protected <T extends AbstractFSWindowStateManager> List<T> createPartitions(int newCount,
      Set<Integer> removedOperatorIds)
  {
    repartitioned = true;

    @SuppressWarnings("unchecked")
    KryoCloneUtils<T> cloneUtils = (KryoCloneUtils<T>)KryoCloneUtils.createCloneUtils(this);

    T[] stateManagers = cloneUtils.getClones(newCount);
    if (removedOperatorIds != null && !removedOperatorIds.isEmpty()) {
      stateManagers[0].deletedOperators = removedOperatorIds;
    }

    List<T> managers = new ArrayList<>();
    managers.addAll(Arrays.asList(stateManagers));
    return managers;
  }

  protected void setRelyOnCheckpoints(boolean relyOnCheckpoints)
  {
    this.relyOnCheckpoints = relyOnCheckpoints;
  }

  /**
   * @return wal instance
   */
  protected FSWindowReplayWAL getWal()
  {
    return wal;
  }

  @VisibleForTesting
  public Set<Integer> getDeletedOperators()
  {
    if (deletedOperators == null) {
      return null;
    }
    return ImmutableSet.copyOf(deletedOperators);
  }

  /**
   * @return recovery filePath
   */
  public String getStatePath()
  {
    return statePath;
  }

  /**
   * Sets the state path. If {@link #isStatePathRelativeToAppPath} is true then this filePath is handled
   * relative
   * to the application filePath; otherwise it is handled as an absolute filePath.
   *
   * @param statePath recovery filePath
   */
  public void setStatePath(String statePath)
  {
    this.statePath = statePath;
  }

  /**
   * @return true if state path is relative to app path; false otherwise.
   */
  public boolean isStatePathRelativeToAppPath()
  {
    return isStatePathRelativeToAppPath;
  }

  /**
   * Specifies whether the state path is relative to application filePath.
   *
   * @param statePathRelativeToAppPath true if state path is relative to application path; false
   *                                      otherwise.
   */
  public void setStatePathRelativeToAppPath(boolean statePathRelativeToAppPath)
  {
    isStatePathRelativeToAppPath = statePathRelativeToAppPath;
  }

  /**
   * Helper method that catches IOException while reading from wal to check if an entry was saved completely or not.
   * @param reader wal reader
   * @return wal entry
   */
  protected static Slice readNext(FileSystemWAL.FileSystemWALReader reader)
  {
    try {
      return reader.next();
    } catch (IOException ex) {
      //exception while reading wal entry which can be because there may have been failure while persisting an
      //artifact so this window is not a finished window.
      try {
        reader.close();
      } catch (IOException ioe) {
        //closing the reader quietly.
      }
      return null;
    }
  }

  /**
   * Helper method that catches IOException while skipping an entry from wal to check if an entry was saved
   * completely or not.
   * @param reader wal reader
   * @return true if skip was successful; false otherwise.
   */
  protected static boolean skipNext(FileSystemWAL.FileSystemWALReader reader)
  {
    try {
      reader.skipNext();
      return true;
    } catch (IOException ex) {
      //exception while skipping wal entry which can be because there may have been failure while persisting an
      //artifact so this window is not a finished window.
      try {
        reader.close();
      } catch (IOException e) {
        //closing the reader quietly
      }
      return false;
    }
  }

  /**
   * Helper method that appends window id to filesystem wal
   * @param writer    filesystem wal writer
   * @param windowId  long window id
   */
  protected static void appendWindowId(FileSystemWAL.FileSystemWALWriter writer, long windowId) throws IOException
  {
    byte[] windowIdBytes = Longs.toByteArray(windowId);
    writer.append(new Slice(windowIdBytes));
  }

}
