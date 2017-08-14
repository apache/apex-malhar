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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Statistics;
import org.apache.kudu.client.Update;
import org.apache.kudu.client.Upsert;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An Abstract operator that would allow concrete implementations to write a POJO value into a given Kudu table.
 * <p>
 * To use this operator, the following needs to be done by the implementor
 * <ol>
 * <li>Create a concrete implementation of this operator and implement the method to define the connection
 * properties to Kudu. The connection properties are set using the {@link ApexKuduConnection} using a builder pattern.
 * </li>
 * <li>Implement the logic how tuples need to ne handled in the event of a reconciliation phase ( i.e. when an
 * operator is resuming back from failure ). See javadoc of the method for more details.</li>
 * <li>Define the payload class</li>
 * </ol>
 * </p>
 * <p>
 * Note that the tuple that is getting processed is not the POJO class. The tuple that is getting processed is the
 * {@link KuduExecutionContext} instance. This is because the operator supports mutation types as a higher level
 * construct than simply writing a POJO into a Kudu table row.
 * </p>
 * <p>
 * Supported mutations are:
 * <ol>
 *      <li>INSERT</li>
 *      <li>UPDATE</li>
 *      <li>UPSERT</li>
 *      <li>DELETE</li>
 * </ol>
 * </p>
 * <p>
 * Please note that the Update mutation allows to change a subset of columns only even though there might be columns
 * that were defined to be non-nullable. This is because the original mutation of type insert would have written the
 * mandatory columns. In such scenarios, the method setDoNotWriteColumns() in {@link KuduExecutionContext} can be
 * used to specify only those columns that need an update. This ways a read and Update pattern can be merged to a
 * simple update pattern thus avoiding a read if required.</p>
 *
 * @since 3.8.0
 * */
@InterfaceStability.Evolving
public abstract class AbstractKuduOutputOperator extends BaseOperator
    implements Operator.ActivationListener<Context.OperatorContext>,Operator.CheckpointNotificationListener
{

  private transient ApexKuduConnection apexKuduConnection;

  private transient  KuduTable kuduTable;

  private transient  KuduSession kuduSession;

  private transient KuduClient kuduClientHandle;

  private transient Map<String,ColumnSchema> allColumnDefs;

  private transient Map<String,Object> kuduColumnBasedGetters;

  private Set<String> primaryKeyColumnNames;

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractKuduOutputOperator.class);

  @NotNull
  protected WindowDataManager windowDataManager;

  private transient long currentWindowId;

  private transient boolean isInReplayMode;

  private transient boolean isInReconcilingMode;

  private transient long reconcilingWindowId;

  @AutoMetric
  transient long numInserts;

  @AutoMetric
  transient long numUpserts;

  @AutoMetric
  transient long numDeletes;

  @AutoMetric
  transient long numUpdates;

  @AutoMetric
  transient long numOpsErrors;

  @AutoMetric
  transient long numBytesWritten;

  @AutoMetric
  transient long numRpcErrors;

  @AutoMetric
  transient long numWriteOps;

  @AutoMetric
  transient long numWriteRPCs;

  @AutoMetric
  long totalOpsErrors = 0;

  @AutoMetric
  long totalBytesWritten = 0;

  @AutoMetric
  long totalRpcErrors = 0;

  @AutoMetric
  long totalWriteOps = 0;

  @AutoMetric
  long totalWriteRPCs = 0;

  @AutoMetric
  long totalInsertsSinceStart;

  @AutoMetric
  long totalUpsertsSinceStart;

  @AutoMetric
  long totalDeletesSinceStart;

  @AutoMetric
  long totalUpdatesSinceStart;

  public final transient DefaultInputPort<KuduExecutionContext> input = new DefaultInputPort<KuduExecutionContext>()
  {
    @Override
    public void process(KuduExecutionContext kuduExecutionContext)
    {
      processTuple(kuduExecutionContext);
    }
  }; // end input port implementation

  public void processTuple(KuduExecutionContext kuduExecutionContext)
  {
    if ( isInReconcilingMode || isInReplayMode) {
      if ( !isEligibleForPassivationInReconcilingWindow(kuduExecutionContext, currentWindowId)) {
        return;
      }
    }
    KuduMutationType mutationType = kuduExecutionContext.getMutationType();
    switch (mutationType) {
      case DELETE:
        processForDelete(kuduExecutionContext);
        numDeletes += 1;
        totalDeletesSinceStart += 1;
        break;
      case INSERT:
        processForInsert(kuduExecutionContext);
        numInserts += 1;
        totalInsertsSinceStart += 1;
        break;
      case UPDATE:
        processForUpdate(kuduExecutionContext);
        numUpdates += 1;
        totalUpdatesSinceStart += 1;
        break;
      case UPSERT:
        processForUpsert(kuduExecutionContext);
        numUpserts += 1;
        totalUpsertsSinceStart += 1;
        break;
      default:
        break;
    }
  }

  /***
   * Sets the values from the Pojo into the Kudu mutation object.
   * @param currentOperation The operation instance that represents the current mutation. This will be applied to the
   *                         current session
   * @param kuduExecutionContext The tuple that contains the payload as well as other information like mutation type etc
   */
  @SuppressWarnings(value = "unchecked")
  private void performCommonProcessing(Operation currentOperation, KuduExecutionContext kuduExecutionContext)
  {
    currentOperation.setExternalConsistencyMode(kuduExecutionContext.getExternalConsistencyMode());
    Long propagatedTimeStamp = kuduExecutionContext.getPropagatedTimestamp();
    if ( propagatedTimeStamp != null) { // set propagation timestamp only if enabled
      currentOperation.setPropagatedTimestamp(propagatedTimeStamp);
    }
    PartialRow partialRow = currentOperation.getRow();
    Object payload = kuduExecutionContext.getPayload();
    Set<String> doNotWriteColumns = kuduExecutionContext.getDoNotWriteColumns();
    if (doNotWriteColumns == null) {
      doNotWriteColumns = new HashSet<>();
    }
    for (String columnName: kuduColumnBasedGetters.keySet()) {
      if ( doNotWriteColumns.contains(columnName)) {
        continue;
      }
      ColumnSchema columnSchema = allColumnDefs.get(columnName);
      Type dataType = columnSchema.getType();
      try {
        switch (dataType) {
          case STRING:
            PojoUtils.Getter<Object, String> stringGetter = ((PojoUtils.Getter<Object, String>)kuduColumnBasedGetters
                .get(columnName));
            if (stringGetter != null) {
              final String stringValue = stringGetter.get(payload);
              if (stringValue != null) {
                partialRow.addString(columnName, stringValue);
              }
            }
            break;
          case BINARY:
            PojoUtils.Getter<Object, ByteBuffer> byteBufferGetter = ((PojoUtils.Getter<Object, ByteBuffer>)
                kuduColumnBasedGetters.get(columnName));
            if (byteBufferGetter != null) {
              final ByteBuffer byteBufferValue = byteBufferGetter.get(payload);
              if (byteBufferValue != null) {
                partialRow.addBinary(columnName, byteBufferValue);
              }
            }
            break;
          case BOOL:
            PojoUtils.GetterBoolean<Object> boolGetter = ((PojoUtils.GetterBoolean<Object>)kuduColumnBasedGetters.get(
                columnName));
            if (boolGetter != null) {
              final boolean boolValue = boolGetter.get(payload);
              partialRow.addBoolean(columnName, boolValue);
            }
            break;
          case DOUBLE:
            PojoUtils.GetterDouble<Object> doubleGetter = ((PojoUtils.GetterDouble<Object>)kuduColumnBasedGetters.get(
                columnName));
            if (doubleGetter != null) {
              final double doubleValue = doubleGetter.get(payload);
              partialRow.addDouble(columnName, doubleValue);
            }
            break;
          case FLOAT:
            PojoUtils.GetterFloat<Object> floatGetter = ((PojoUtils.GetterFloat<Object>)kuduColumnBasedGetters.get(
                columnName));
            if (floatGetter != null) {
              final float floatValue = floatGetter.get(payload);
              partialRow.addFloat(columnName, floatValue);
            }
            break;
          case INT8:
            PojoUtils.GetterByte<Object> byteGetter = ((PojoUtils.GetterByte<Object>)kuduColumnBasedGetters.get(
                columnName));
            if (byteGetter != null) {
              final byte byteValue = byteGetter.get(payload);
              partialRow.addByte(columnName, byteValue);
            }
            break;
          case INT16:
            PojoUtils.GetterShort<Object> shortGetter = ((PojoUtils.GetterShort<Object>)kuduColumnBasedGetters.get(
                columnName));
            if (shortGetter != null) {
              final short shortValue = shortGetter.get(payload);
              partialRow.addShort(columnName, shortValue);
            }
            break;
          case INT32:
            PojoUtils.GetterInt<Object> intGetter = ((PojoUtils.GetterInt<Object>)
                kuduColumnBasedGetters.get(columnName));
            if (intGetter != null) {
              final int intValue = intGetter.get(payload);
              partialRow.addInt(columnName, intValue);
            }
            break;
          case INT64:
          case UNIXTIME_MICROS:
            PojoUtils.GetterLong<Object> longGetter = ((PojoUtils.GetterLong<Object>)kuduColumnBasedGetters.get(
                columnName));
            if (longGetter != null) {
              final long longValue = longGetter.get(payload);
              partialRow.addLong(columnName, longValue);
            }
            break;
          default:
            LOG.error(columnName + " is not of the supported data type");
            throw new UnsupportedOperationException("Kudu does not support data type for column " + columnName);
        }
      } catch ( Exception ex ) {
        LOG.error(" Exception while fetching the value of " + columnName + " because " + ex.getMessage());
        partialRow.setNull(columnName);
      }
    }
    try {
      kuduSession.apply(currentOperation);
    } catch (KuduException e) {
      throw new RuntimeException("Could not execute operation because " + e.getMessage(), e);
    }
  }

  protected void processForUpdate(KuduExecutionContext kuduExecutionContext)
  {
    Update thisUpdate = kuduTable.newUpdate();
    performCommonProcessing(thisUpdate,kuduExecutionContext);
  }


  protected void processForUpsert(KuduExecutionContext kuduExecutionContext)
  {
    Upsert thisUpsert = kuduTable.newUpsert();
    performCommonProcessing(thisUpsert,kuduExecutionContext);
  }



  protected void processForDelete(KuduExecutionContext kuduExecutionContext)
  {
    Delete thisDelete = kuduTable.newDelete();
    // Kudu does not allow column values to be set in case of a delete mutation
    Set<String> doNotWriteCols = kuduExecutionContext.getDoNotWriteColumns();
    if ( doNotWriteCols == null) {
      doNotWriteCols = new HashSet<>();
    }
    doNotWriteCols.clear();
    for (String columnName : allColumnDefs.keySet()) {
      if ( !(primaryKeyColumnNames.contains(columnName))) {
        doNotWriteCols.add(columnName);
      }
    }
    kuduExecutionContext.setDoNotWriteColumns(doNotWriteCols);
    performCommonProcessing(thisDelete,kuduExecutionContext);
  }


  protected void processForInsert(KuduExecutionContext kuduExecutionContext)
  {
    Insert thisInsert = kuduTable.newInsert();
    performCommonProcessing(thisInsert,kuduExecutionContext);
  }

  @Override
  public void activate(Context.OperatorContext context)
  {
    ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnectionBuilder = getKuduConnectionConfig();
    apexKuduConnection = apexKuduConnectionBuilder.build();
    checkNotNull(apexKuduConnection,"Kudu connection cannot be null");
    kuduTable = apexKuduConnection.getKuduTable();
    kuduSession = apexKuduConnection.getKuduSession();
    kuduClientHandle = apexKuduConnection.getKuduClient();
    checkNotNull(kuduTable,"Kudu Table cannot be null");
    checkNotNull(kuduSession, "Kudu session cannot be null");
    allColumnDefs = new HashMap();
    primaryKeyColumnNames = new HashSet<>();
    kuduColumnBasedGetters = new HashMap();
    buildGettersForPojoPayload();
    reconcilingWindowId = Stateless.WINDOW_ID;
    // The operator is working in a replay mode where the upstream buffer is re-streaming the tuples
    // Note that there are two windows that need special core. The window that is being replayed and the subsequent
    // window that might have resulted in a crash which we are referring as reconciling window
    if ( (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID) &&
        context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) <
        windowDataManager.getLargestCompletedWindow()) {
      reconcilingWindowId = windowDataManager.getLargestCompletedWindow() + 1;
    }

    if ( (context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID) &&
        context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) ==
        windowDataManager.getLargestCompletedWindow()) {
      reconcilingWindowId = windowDataManager.getLargestCompletedWindow();
    }
  }

  @Override
  public void deactivate()
  {
    try {
      apexKuduConnection.close();
    } catch (Exception e) {
      LOG.error("Could not close kudu session and resources because " + e.getMessage(), e);
    }
  }

  @Override
  public void beforeCheckpoint(long l)
  {
    // Nothing to be done here. Child classes can use this if required
  }

  @Override
  public void checkpointed(long l)
  {
    // Nothing to be done here. Child classes can use this if required
  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException("Error while committing the window id " + windowId + " because " + e.getMessage(), e );
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    windowDataManager = getWindowDataManager();
    if ( windowDataManager == null) {
      windowDataManager = new FSWindowDataManager();
    }
    windowDataManager.setup(context);
  }

  @Override
  public void teardown()
  {
    windowDataManager.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    currentWindowId = windowId;
    if ( currentWindowId != Stateless.WINDOW_ID) { // if it is not the first window of the application
      if (currentWindowId > reconcilingWindowId) {
        isInReplayMode = false;
        isInReconcilingMode = false;
      }
      if (currentWindowId == reconcilingWindowId) {
        isInReconcilingMode = true;
        isInReplayMode = false;
      }
      if (currentWindowId < reconcilingWindowId) {
        isInReconcilingMode = false;
        isInReplayMode = true;
      }
    }
    numDeletes = 0;
    numInserts = 0;
    numUpdates = 0;
    numUpserts = 0;
  }

  @Override
  public void endWindow()
  {
    try {
      kuduSession.flush();
    } catch (KuduException e) {
      throw new RuntimeException("Could not flush kudu session on an end window boundary " + e.getMessage(), e);
    }
    if ( currentWindowId > windowDataManager.getLargestCompletedWindow()) {
      try {
        windowDataManager.save(currentWindowId,currentWindowId);
      } catch (IOException e) {
        throw new RuntimeException("Error while persisting the current window state " + currentWindowId +
            " because " + e.getMessage(), e);
      }
    }
    numOpsErrors = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.OPS_ERRORS) -
      totalOpsErrors;
    numBytesWritten = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.BYTES_WRITTEN) -
      totalBytesWritten;
    numRpcErrors = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.RPC_ERRORS) -
      totalRpcErrors;
    numWriteOps = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.WRITE_OPS) -
      totalWriteOps;
    numWriteRPCs = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.WRITE_RPCS) - totalWriteOps;
    totalOpsErrors = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.OPS_ERRORS);
    totalBytesWritten = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.BYTES_WRITTEN);
    totalRpcErrors = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.RPC_ERRORS);
    totalWriteOps = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.WRITE_OPS);
    totalWriteRPCs = kuduClientHandle.getStatistics().getClientStatistic(Statistics.Statistic.WRITE_RPCS);
  }

  private void buildGettersForPojoPayload()
  {
    Class payloadClass = getTuplePayloadClass();
    checkNotNull(payloadClass,"Payload class cannot be null");
    Field[] classFields = payloadClass.getDeclaredFields();
    Schema schemaInfo = kuduTable.getSchema();
    List<ColumnSchema> allColumns = schemaInfo.getColumns();
    Set<String> allKuduTableColumnNames = new HashSet<>();
    Map<String,ColumnSchema> normalizedColumns = new HashMap();
    for ( ColumnSchema aColumnDef : allColumns) {
      allColumnDefs.put(aColumnDef.getName(), aColumnDef);
      normalizedColumns.put(aColumnDef.getName().toLowerCase(), aColumnDef);
      allKuduTableColumnNames.add(aColumnDef.getName().toLowerCase());
    }
    List<ColumnSchema> primaryKeyColumns = schemaInfo.getPrimaryKeyColumns();
    for (ColumnSchema primaryKeyInfo : primaryKeyColumns) {
      primaryKeyColumnNames.add(primaryKeyInfo.getName());
    }
    Map<String,String> columnNameOverrides = getOverridingColumnNameMap();
    if (columnNameOverrides == null) {
      columnNameOverrides = new HashMap(); // to avoid null checks further down the line
    }
    for ( Field aFieldDef : classFields) {
      String currentFieldName = aFieldDef.getName().toLowerCase();
      if (allKuduTableColumnNames.contains(currentFieldName)) {
        extractGetterForColumn(normalizedColumns.get(currentFieldName),aFieldDef);
      } else {
        if (columnNameOverrides.containsKey(aFieldDef.getName())) {
          extractGetterForColumn(normalizedColumns.get(columnNameOverrides.get(aFieldDef.getName()).toLowerCase()),
              aFieldDef);
        } else if (columnNameOverrides.containsKey(aFieldDef.getName().toLowerCase())) {
          extractGetterForColumn(normalizedColumns.get(columnNameOverrides.get(aFieldDef.getName().toLowerCase())
              .toLowerCase()),aFieldDef);
        }
      }
    }
  }

  /***
   * Used to build a getter for the given schema column from the POJO field definition
   * @param columnSchema The Kudu column definition
   * @param fieldDefinition The POJO field definition
   */
  private void extractGetterForColumn(ColumnSchema columnSchema, Field fieldDefinition)
  {
    Type columnType = columnSchema.getType();
    Class pojoClass = getTuplePayloadClass();
    Object getter = null;
    switch ( columnType ) {
      case BINARY:
        getter = PojoUtils.createGetter(pojoClass, fieldDefinition.getName(), ByteBuffer.class);
        break;
      case STRING:
        getter = PojoUtils.createGetter(pojoClass, fieldDefinition.getName(), String.class);
        break;
      case BOOL:
        getter = PojoUtils.createGetterBoolean(pojoClass, fieldDefinition.getName());
        break;
      case DOUBLE:
        getter = PojoUtils.createGetterDouble(pojoClass, fieldDefinition.getName());
        break;
      case FLOAT:
        getter = PojoUtils.createGetterFloat(pojoClass, fieldDefinition.getName());
        break;
      case INT8:
        getter = PojoUtils.createGetterByte(pojoClass, fieldDefinition.getName());
        break;
      case INT16:
        getter = PojoUtils.createGetterShort(pojoClass, fieldDefinition.getName());
        break;
      case INT32:
        getter = PojoUtils.createGetterInt(pojoClass, fieldDefinition.getName());
        break;
      case INT64:
      case UNIXTIME_MICROS:
        getter = PojoUtils.createGetterLong(pojoClass, fieldDefinition.getName());
        break;
      default:
        LOG.error(fieldDefinition.getName() + " has a data type that is not yet supported");
        throw new UnsupportedOperationException(fieldDefinition.getName() + " does not have a compatible data type");
    }
    if (getter != null) {
      kuduColumnBasedGetters.put(columnSchema.getName(),getter);
    }
  }


  public static ApexKuduConnection.ApexKuduConnectionBuilder usingKuduConnectionBuilder()
  {
    return new ApexKuduConnection.ApexKuduConnectionBuilder();
  }

  public WindowDataManager getWindowDataManager()
  {
    return windowDataManager;
  }

  public void setWindowDataManager(WindowDataManager windowDataManager)
  {
    this.windowDataManager = windowDataManager;
  }

  /***
   * Allows to map a POJO field name to a Kudu Table column name. This is useful in case
   * the POJO field name can't be changed to an unconventional name ( ex: if kudu column names have underscores ). It
   * can be also useful when the developer does not want to declare a new POJO but reuse an existing POJO.
   * Note that the key in the map is the name of the field in the POJO and
   * the value part is used to denote the name of the kudu column
   * @return The map giving the mapping from POJO field name to the Kudu column name
   */
  protected Map<String,String> getOverridingColumnNameMap()
  {
    return new HashMap<>();
  }

  abstract ApexKuduConnection.ApexKuduConnectionBuilder getKuduConnectionConfig();

  /***
   * Represents the Tuple payload class that maps to a Kudu table row. Note that the POJO fields are mapped to the
   * kudu column names is a lenient way. For example, the mapping of POJO field names to the kudu columns is done
   * in a case-insensitive way.
   * @return The class that will be used to map to a row in the given Kudu table.
   */
  protected abstract Class getTuplePayloadClass();

  /***
   * This is used to give control to the concrete implementation of this operator how to resolve whether to write a
   * mutation into a given kudu table in the event of a failure and the operator subsequently resumes. This window
   * is marked as a reconciling window. It is only for this reconciling window that we need to give control to the
   * concrete operator implementor how to actually resolve if the entry needs to be excuted as a mutation in Kudu.
   * @param executionContext The tuple which represents the execution context along with the payload
   * @param reconcilingWindowId The window Id of the reconciling window
   * @return true if we would like the entry to result in a mutation in the Kudu table.
   */
  protected abstract boolean isEligibleForPassivationInReconcilingWindow(KuduExecutionContext executionContext,
      long reconcilingWindowId);
}

