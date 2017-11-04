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
package org.apache.apex.malhar.kudu;



import java.util.HashSet;
import java.util.Set;

import org.apache.kudu.client.ExternalConsistencyMode;

/**
 * <p>Represents a summary of the mutation that needs to be done on the Kudu table. The type of mutation is
 * decided by the KuduMutation Type field. The actual data that is mutated inside the kudu table row is
 * represented by the payload. The execution context itself a templated class based on the payload class.</p>
 *
 * @since 3.8.0
 */
public class KuduExecutionContext<T>
{
  private T payload;
  /***
   * <p>Represents the set of columns that are not to be written to a Kudu row. Note that this is useful when we
   * would like to not write a set of columns into the table either because
   * <ol>
   * <li>We are doing an update and we would like to update only a few of the columns as the original columns were
   * already written in the original insert mutation</li>
   * <li>When we would like to not write a column because the column is an optional column as per the schema definition
   * </li>
   * </ol>
   * It may be noted that the client driver will throw an exception when a mandatory column is not written.</p>
   */
  private Set<String> doNotWriteColumns = new HashSet<>();

  private KuduMutationType mutationType = KuduMutationType.UPSERT;

  private ExternalConsistencyMode externalConsistencyMode;

  private Long propagatedTimestamp;

  public T getPayload()
  {
    return payload;
  }

  public void setPayload(T payload)
  {
    this.payload = payload;
  }

  public KuduMutationType getMutationType()
  {
    return mutationType;
  }

  public void setMutationType(KuduMutationType mutationType)
  {
    this.mutationType = mutationType;
  }

  public ExternalConsistencyMode getExternalConsistencyMode()
  {
    return externalConsistencyMode;
  }

  public void setExternalConsistencyMode(ExternalConsistencyMode externalConsistencyMode)
  {
    this.externalConsistencyMode = externalConsistencyMode;
  }

  public Long getPropagatedTimestamp()
  {
    return propagatedTimestamp;
  }

  public void setPropagatedTimestamp(Long propagatedTimestamp)
  {
    this.propagatedTimestamp = propagatedTimestamp;
  }

  public Set<String> getDoNotWriteColumns()
  {
    return doNotWriteColumns;
  }

  public void setDoNotWriteColumns(Set<String> doNotWriteColumns)
  {
    this.doNotWriteColumns = doNotWriteColumns;
  }

  public void addDoNotWriteColumn(String aKuduColumnName)
  {
    doNotWriteColumns.add(aKuduColumnName);
  }
}
