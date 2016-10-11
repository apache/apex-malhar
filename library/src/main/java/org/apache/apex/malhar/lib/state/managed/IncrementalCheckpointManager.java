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
package org.apache.apex.malhar.lib.state.managed;

import java.io.IOException;
import java.util.Map;

import com.datatorrent.netlet.util.Slice;

/**
 * {@link IncrementalCheckpointManager} persists change in state as opposed to the complete state at an
 * intermediate location.<br/>
 * The state is then asynchronously transferred from intermediate location to {@link BucketsFileSystem} which is
 * efficient for reading by {@link ManagedState} implementations.
 * <p/>
 * A concrete implementation of this- {@link IncrementalCheckpointManagerImpl} persists incremental state to
 * {@link org.apache.apex.malhar.lib.wal.FileSystemWAL}.
 */
public interface IncrementalCheckpointManager extends ManagedStateComponent, TimeBucketAssigner.PurgeListener
{
  /**
   * Retrieve bucket data of all the windows {@link com.datatorrent.api.Context.OperatorContext#ACTIVATION_WINDOW_ID}
   * saved in the intermediate location.<br/>
   *
   * @return bucket data per window recovered from intermediate location after failure.
   * @throws IOException
   */
  Map<Long, Map<Long, Map<Slice, Bucket.BucketedValue>>> retrieveAllWindows() throws IOException;

  /**
   * Write the bucket data wrt windowId to intermediate location.
   * @param windowId window id.
   * @param buckets  bucket data.
   * @throws IOException
   */
  void writeBuckets(long windowId, Map<Long, Map<Slice, Bucket.BucketedValue>> buckets) throws IOException;

  /**
   * Notifies the {@link IncrementalCheckpointManager} before a checkpoint is performed.<br/>
   * {@link IncrementalCheckpointManagerImpl} uses this to forcefully flush the data to wal.
   *
   * @param windowId window id
   */
  void beforeCheckpoint(long windowId);

  /**
   * Notifies the {@link IncrementalCheckpointManager} that a window is committed.
   * @param windowId window id.
   */
  void committed(long windowId);

  /**
   * Returns the latest window id, the data for which has been transferred to {@link BucketsFileSystem}.
   *
   * @return last window id, data for which is transferred from intermediate location to {@link BucketsFileSystem}
   */
  long getLastTransferredWindow();
}
