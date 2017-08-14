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

import java.util.Comparator;

import org.apache.apex.malhar.lib.fileaccess.FileAccess;
import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.apex.malhar.lib.util.comparator.SliceComparator;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

class MockManagedStateContext implements ManagedStateContext
{
  private TFileImpl.DTFileImpl fileAccess = new TFileImpl.DTFileImpl();
  private Comparator<Slice> keyComparator = new SliceComparator();
  private BucketsFileSystem bucketsFileSystem = new BucketsFileSystem();
  private MovingBoundaryTimeBucketAssigner timeBucketAssigner = new MovingBoundaryTimeBucketAssigner();

  private final Context.OperatorContext operatorContext;

  public MockManagedStateContext(Context.OperatorContext operatorContext)
  {
    this.operatorContext = operatorContext;
  }

  @Override
  public FileAccess getFileAccess()
  {
    return fileAccess;
  }

  @Override
  public Comparator<Slice> getKeyComparator()
  {
    return keyComparator;
  }

  public BucketsFileSystem getBucketsFileSystem()
  {
    return bucketsFileSystem;
  }

  @Override
  public MovingBoundaryTimeBucketAssigner getTimeBucketAssigner()
  {
    return timeBucketAssigner;
  }

  @Override
  public Context.OperatorContext getOperatorContext()
  {
    return operatorContext;
  }

  void setFileAccess(TFileImpl.DTFileImpl fileAccess)
  {
    this.fileAccess = fileAccess;
  }

  void setKeyComparator(Comparator<Slice> keyComparator)
  {
    this.keyComparator = keyComparator;
  }

  void setBucketsFileSystem(BucketsFileSystem bucketsFileSystem)
  {
    this.bucketsFileSystem = bucketsFileSystem;
  }

  void setTimeBucketAssigner(MovingBoundaryTimeBucketAssigner timeBucketAssigner)
  {
    this.timeBucketAssigner = timeBucketAssigner;
  }
}
