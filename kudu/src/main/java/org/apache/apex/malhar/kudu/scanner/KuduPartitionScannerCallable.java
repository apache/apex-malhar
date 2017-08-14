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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.ApexKuduConnection;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.sqltranslator.SQLToKuduPredicatesTranslator;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import static com.google.common.base.Preconditions.checkNotNull;

/***
 * A callable implementation that is responsible for scanning all rows that are scannable from a given
 * scan token range from a single Kudu tablet. The rows are sent to the input operator buffer so that they
 * can be emitted as windows progress.
 */
/**
 * @since 3.8.0
 */
public class KuduPartitionScannerCallable<T,C extends InputOperatorControlTuple> implements Callable<Long>
{
  private AbstractKuduInputOperator<T,C> operatorUsingThisScanner;

  private KuduPartitionScanAssignmentMeta kuduPartitionScanAssignmentMeta;

  private BlockingQueue<KuduRecordWithMeta<T>> bufferForTransmittingRecords;

  private Class<T> clazzForResultObject;

  private transient KuduClient kuduClientHandle;

  private Map<String,ColumnSchema> tableSchema;

  private Map<String,Object> settersForThisQueryScan;

  private SQLToKuduPredicatesTranslator parsedQuery;


  private static final Logger LOG = LoggerFactory.getLogger(KuduPartitionScannerCallable.class);

  public KuduPartitionScannerCallable(AbstractKuduInputOperator<T,C> kuduInputOperator,
      KuduPartitionScanAssignmentMeta partitionMeta, ApexKuduConnection apexKuduConnection,Map<String,Object> setters,
      SQLToKuduPredicatesTranslator parsedQueryInstance)
  {
    checkNotNull(kuduInputOperator,"Kudu operator instance cannot be null in the kudu scanner thread");
    checkNotNull(partitionMeta, "Partition metadata cannot be null in kudu scanner thread");
    checkNotNull(apexKuduConnection,"Kudu connection cannot be null in the kudu scanner thread");
    checkNotNull(apexKuduConnection,"Setters cannot be null in the kudu scanner thread");
    checkNotNull(parsedQueryInstance, "parsed Query instance cannot be null");
    operatorUsingThisScanner = kuduInputOperator;
    kuduPartitionScanAssignmentMeta = partitionMeta;
    bufferForTransmittingRecords = kuduInputOperator.getBuffer();
    clazzForResultObject = kuduInputOperator.getClazzForResultObject();
    checkNotNull(apexKuduConnection,"Kudu connection cannot be null when initializing scanner");
    kuduClientHandle = apexKuduConnection.getKuduClient();
    checkNotNull(kuduClientHandle,"Kudu client cannot be null when initializing scanner");
    tableSchema = kuduInputOperator.getKuduColNameToSchemaMapping();
    settersForThisQueryScan = setters;
    parsedQuery = parsedQueryInstance;
  }

  public void setValuesInPOJO(RowResult aRow, T payload)
  {
    Set<String> columnsUsed = parsedQuery.getKuduSQLParseTreeListener().getListOfColumnsUsed();
    for (String aColumnName : columnsUsed) {
      ColumnSchema schemaForThisColumn = tableSchema.get(aColumnName);
      if (aRow.isNull(aColumnName)) {
        continue;
      }
      switch ( schemaForThisColumn.getType().getDataType().getNumber()) {
        case Common.DataType.BINARY_VALUE:
          ((PojoUtils.Setter<T,ByteBuffer>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getBinary(aColumnName));
          break;
        case Common.DataType.STRING_VALUE:
          ((PojoUtils.Setter<T,String>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getString(aColumnName));
          break;
        case Common.DataType.BOOL_VALUE:
          ((PojoUtils.SetterBoolean<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getBoolean(aColumnName));
          break;
        case Common.DataType.DOUBLE_VALUE:
          ((PojoUtils.SetterDouble<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getDouble(aColumnName));
          break;
        case Common.DataType.FLOAT_VALUE:
          ((PojoUtils.SetterFloat<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getFloat(aColumnName));
          break;
        case Common.DataType.INT8_VALUE:
          ((PojoUtils.SetterByte<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getByte(aColumnName));
          break;
        case Common.DataType.INT16_VALUE:
          ((PojoUtils.SetterShort<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getShort(aColumnName));
          break;
        case Common.DataType.INT32_VALUE:
          ((PojoUtils.SetterInt<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getInt(aColumnName));
          break;
        case Common.DataType.UNIXTIME_MICROS_VALUE:
        case Common.DataType.INT64_VALUE:
          ((PojoUtils.SetterLong<T>)settersForThisQueryScan.get(aColumnName)).set(
              payload,aRow.getLong(aColumnName));
          break;
        case Common.DataType.UINT8_VALUE:
          LOG.error("Unsigned int 8 not supported yet");
          throw new RuntimeException("uint8 not supported in Kudu schema yet");
        case Common.DataType.UINT16_VALUE:
          LOG.error("Unsigned int 16 not supported yet");
          throw new RuntimeException("uint16 not supported in Kudu schema yet");
        case Common.DataType.UINT32_VALUE:
          LOG.error("Unsigned int 32 not supported yet");
          throw new RuntimeException("uint32 not supported in Kudu schema yet");
        case Common.DataType.UINT64_VALUE:
          LOG.error("Unsigned int 64 not supported yet");
          throw new RuntimeException("uint64 not supported in Kudu schema yet");
        case Common.DataType.UNKNOWN_DATA_VALUE:
          LOG.error("unknown data type ( complex types ? )  not supported yet");
          throw new RuntimeException("Unknown data type  ( complex types ? ) not supported in Kudu schema yet");
        default:
          LOG.error("unknown type/default  ( complex types ? )  not supported yet");
          throw new RuntimeException("Unknown type/default  ( complex types ? ) not supported in Kudu schema yet");
      }
    }
  }

  @Override
  public Long call() throws Exception
  {
    long numRowsScanned = 0;
    KuduScanner aPartitionSpecificScanner = KuduScanToken.deserializeIntoScanner(
        kuduPartitionScanAssignmentMeta.getSerializedKuduScanToken(), kuduClientHandle);
    LOG.info("Scanning the following tablet " + KuduScanToken.stringifySerializedToken(kuduPartitionScanAssignmentMeta
        .getSerializedKuduScanToken(), kuduClientHandle));
    KuduRecordWithMeta<T> beginScanRecord = new KuduRecordWithMeta<>();
    beginScanRecord.setBeginScanMarker(true);
    beginScanRecord.setTabletMetadata(kuduPartitionScanAssignmentMeta);
    bufferForTransmittingRecords.add(beginScanRecord); // Add a record entry that denotes the end of this scan.
    while ( aPartitionSpecificScanner.hasMoreRows()) {
      LOG.debug("Number of columns being returned for this read " +
          aPartitionSpecificScanner.getProjectionSchema().getColumnCount());
      RowResultIterator resultIterator = aPartitionSpecificScanner.nextRows();
      if (resultIterator == null) {
        break;
      } else {
        while (resultIterator.hasNext()) {
          KuduRecordWithMeta<T> recordWithMeta = new KuduRecordWithMeta<>();
          RowResult aRow = resultIterator.next();
          recordWithMeta.setPositionInScan(numRowsScanned);
          T payload = clazzForResultObject.newInstance();
          recordWithMeta.setThePayload(payload);
          recordWithMeta.setEndOfScanMarker(false);
          recordWithMeta.setTabletMetadata(kuduPartitionScanAssignmentMeta);
          setValuesInPOJO(aRow,payload);
          bufferForTransmittingRecords.add(recordWithMeta);
          numRowsScanned += 1;
        }
      }
    }
    aPartitionSpecificScanner.close();
    KuduRecordWithMeta<T> endScanRecord = new KuduRecordWithMeta<>();
    endScanRecord.setEndOfScanMarker(true);
    endScanRecord.setTabletMetadata(kuduPartitionScanAssignmentMeta);
    bufferForTransmittingRecords.add(endScanRecord); // Add a record entry that denotes the end of this scan.
    LOG.info(" Scanned a total of " + numRowsScanned + " for this scanner thread @tablet " +
        KuduScanToken.stringifySerializedToken(kuduPartitionScanAssignmentMeta.getSerializedKuduScanToken(),
        kuduClientHandle));
    return numRowsScanned;
  }
}
