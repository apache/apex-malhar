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
package org.apache.apex.malhar.kudu.scanner;

/**
 * Represents a Kudu row and metadata for the row that was consumed by the Kudu scanner client.
 * The metadata includes information like the ordinal position for this scan instance, the query and the scan
 * token was scheduled.
 *
 * @since 3.8.0
 */
public class KuduRecordWithMeta<T>
{
  private T thePayload;

  private long positionInScan;

  private boolean isEndOfScanMarker;

  private boolean isBeginScanMarker;

  private KuduPartitionScanAssignmentMeta tabletMetadata;


  public T getThePayload()
  {
    return thePayload;
  }

  public void setThePayload(T thePayload)
  {
    this.thePayload = thePayload;
  }

  public long getPositionInScan()
  {
    return positionInScan;
  }

  public void setPositionInScan(long positionInScan)
  {
    this.positionInScan = positionInScan;
  }

  public boolean isEndOfScanMarker()
  {
    return isEndOfScanMarker;
  }

  public void setEndOfScanMarker(boolean endOfScanMarker)
  {
    isEndOfScanMarker = endOfScanMarker;
  }

  public KuduPartitionScanAssignmentMeta getTabletMetadata()
  {
    return tabletMetadata;
  }

  public void setTabletMetadata(KuduPartitionScanAssignmentMeta tabletMetadata)
  {
    this.tabletMetadata = tabletMetadata;
  }

  public boolean isBeginScanMarker()
  {
    return isBeginScanMarker;
  }

  public void setBeginScanMarker(boolean beginScanMarker)
  {
    isBeginScanMarker = beginScanMarker;
  }
}
