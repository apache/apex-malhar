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

import java.io.Serializable;
import java.util.Arrays;

/**
 * The main metadata class that is used to hold the information about the scan token and also the ordinal
 * position out of the total.
 *
 * @since 3.8.0
 */
public class KuduPartitionScanAssignmentMeta implements Serializable
{
  private static final long serialVersionUID = -3074453209476815690L;

  private String currentQuery;

  private byte[] serializedKuduScanToken;

  private int ordinal;

  private int totalSize;

  public String getCurrentQuery()
  {
    return currentQuery;
  }

  public void setCurrentQuery(String currentQuery)
  {
    this.currentQuery = currentQuery;
  }

  public byte[] getSerializedKuduScanToken()
  {
    return serializedKuduScanToken;
  }

  public void setSerializedKuduScanToken(byte[] serializedKuduScanToken)
  {
    this.serializedKuduScanToken = serializedKuduScanToken;
  }

  public int getOrdinal()
  {
    return ordinal;
  }

  public void setOrdinal(int ordinal)
  {
    this.ordinal = ordinal;
  }

  public int getTotalSize()
  {
    return totalSize;
  }

  public void setTotalSize(int totalSize)
  {
    this.totalSize = totalSize;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KuduPartitionScanAssignmentMeta)) {
      return false;
    }

    KuduPartitionScanAssignmentMeta that = (KuduPartitionScanAssignmentMeta)o;

    if (getOrdinal() != that.getOrdinal()) {
      return false;
    }
    if (getTotalSize() != that.getTotalSize()) {
      return false;
    }
    return getCurrentQuery().equals(that.getCurrentQuery());
  }

  @Override
  public int hashCode()
  {
    int result = getCurrentQuery().hashCode();
    result = 31 * result + getOrdinal();
    result = 31 * result + getTotalSize();
    return result;
  }

  @Override
  public String toString()
  {
    return "KuduPartitionScanAssignmentMeta{" +
      "currentQuery='" + currentQuery + '\'' +
      ", serializedKuduScanToken=" + Arrays.toString(serializedKuduScanToken) +
      ", ordinal=" + ordinal +
      ", totalSize=" + totalSize +
      '}';
  }
}
