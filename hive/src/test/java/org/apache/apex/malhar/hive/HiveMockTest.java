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
package org.apache.apex.malhar.hive;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;
import org.apache.apex.malhar.hive.FSPojoToHiveOperator.FIELD_TYPE;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.TException;

import io.teknek.hiveunit.HiveTestService;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;

public class HiveMockTest extends HiveTestService
{
  public static final String APP_ID = "HiveOperatorTest";

  public static final int OPERATOR_ID = 0;
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final String tablename = "temp";
  public static final String tablepojo = "temppojo";
  public static final String tablemap = "tempmap";
  public static String delimiterMap = ":";
  public static final String HOST = "localhost";
  public static final String PORT = "10000";
  public static final String DATABASE = "default";
  public static final String HOST_PREFIX = "jdbc:hive://";

  public String testdir;

  private String getDir()
  {
    String filePath = new File("target/hive").getAbsolutePath();
    LOG.info("filepath is {}", filePath);
    return filePath;
  }

  @Override
  public void setUp() throws Exception
  {
    super.setUp();
  }

  private static final int NUMBER_OF_TESTS = 4; // count your tests here
  private int sTestsRun = 0;

  @Override
  public void tearDown() throws Exception
  {
    super.tearDown();
    sTestsRun++;
    //Equivalent of @AfterClass in junit 3.
    if (sTestsRun == NUMBER_OF_TESTS) {
      FileUtils.deleteQuietly(new File(testdir));
    }
  }

  public HiveMockTest() throws IOException, Exception
  {
    super();
    testdir = getDir();
    Properties properties = System.getProperties();
    properties.put("derby.system.home", testdir);
    System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(), testdir);
    new File(testdir).mkdir();
  }

  public static HiveStore createStore(HiveStore hiveStore)
  {
    String host = HOST;
    String port = PORT;

    if (hiveStore == null) {
      hiveStore = new HiveStore();
    }

    StringBuilder sb = new StringBuilder();
    String tempHost = HOST_PREFIX + host + ":" + port;

    LOG.debug("Host name: {}", tempHost);
    LOG.debug("Port: {}", 10000);

    sb.append("user:").append("").append(",");
    sb.append("password:").append("");

    String properties = sb.toString();
    LOG.debug(properties);
    hiveStore.setDatabaseDriver("org.apache.hive.jdbc.HiveDriver");

    hiveStore.setDatabaseUrl("jdbc:hive2://");
    hiveStore.setConnectionProperties(properties);
    return hiveStore;
  }

  public static void hiveInitializeDatabase(HiveStore hiveStore) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    // show tables
    String sql = "show tables";

    LOG.debug(sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      LOG.debug("tables are {}", res.getString(1));
    }

    stmt.execute("DROP TABLE " + tablename);

    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (col1 String) PARTITIONED BY(dt STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'  \n" + "STORED AS TEXTFILE ");
    /*ResultSet res = stmt.execute("CREATE TABLE IF NOT EXISTS temp4 (col1 map<string,int>,col2 map<string,int>,col3  map<string,int>,col4 map<String,timestamp>, col5 map<string,double>,col6 map<string,double>,col7 map<string,int>,col8 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  \n"
     + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
     + "MAP KEYS TERMINATED BY ':'  \n"
     + "LINES TERMINATED BY '\n' "
     + "STORED AS TEXTFILE");*/

    hiveStore.disconnect();
  }

  public static void hiveInitializePOJODatabase(HiveStore hiveStore) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    // show tables
    String sql = "show tables";

    LOG.debug(sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      LOG.debug("tables are {}", res.getString(1));
    }
    stmt.execute("DROP TABLE " + tablepojo);

    stmt.execute("CREATE TABLE " + tablepojo + " (col1 int) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED "
        + "FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' \n" + "STORED AS TEXTFILE ");
    hiveStore.disconnect();
  }

  public static void hiveInitializeMapDatabase(HiveStore hiveStore) throws SQLException
  {
    hiveStore.connect();
    Statement stmt = hiveStore.getConnection().createStatement();
    // show tables
    String sql = "show tables";

    LOG.debug(sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      LOG.debug(res.getString(1));
    }

    stmt.execute("DROP TABLE " + tablemap);

    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablemap
        + " (col1 map<string,int>) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  \n"
        + "MAP KEYS TERMINATED BY '" + delimiterMap + "' \n" + "STORED AS TEXTFILE ");
    hiveStore.disconnect();
  }

  @Test
  public void testInsertString() throws Exception
  {
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testdir);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveInitializeDatabase(createStore(null));
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablename);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    FSRollingTestImpl fsRolling = new FSRollingTestImpl();
    fsRolling.setFilePath(testdir);
    short permission = 511;
    fsRolling.setFilePermission(permission);
    fsRolling.setAlwaysWriteToTmp(false);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-10" + "/" + "0-transaction.out.part.0");
    ArrayList<String> partitions1 = new ArrayList<String>();
    partitions1.add("2014-12-10");
    mapping1.setPartition(partitions1);
    ArrayList<String> partitions2 = new ArrayList<String>();
    partitions2.add("2014-12-11");
    mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-11" + "/" + "0-transaction.out.part.0");
    mapping2.setPartition(partitions2);
    for (int wid = 0, total = 0; wid < NUM_WINDOWS; wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0; tupleCounter < BLAST_SIZE && total < DATABASE_SIZE; tupleCounter++, total++) {
        fsRolling.input.process("2014-12-1" + tupleCounter);
      }
      if (wid == 7) {
        fsRolling.committed(wid - 1);
        hiveOperator.processTuple(mapping1);
        hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();

      if (wid == 6) {
        fsRolling.beforeCheckpoint(wid);
        fsRolling.checkpointed(wid);
      }
    }

    fsRolling.teardown();
    hiveStore.connect();
    client.execute("select * from " + tablename + " where dt='2014-12-10'");
    List<String> recordsInDatePartition1 = client.fetchAll();

    client.execute("select * from " + tablename + " where dt='2014-12-11'");
    List<String> recordsInDatePartition2 = client.fetchAll();
    client.execute("drop table " + tablename);
    hiveStore.disconnect();

    Assert.assertEquals(7, recordsInDatePartition1.size());
    for (int i = 0; i < recordsInDatePartition1.size(); i++) {
      LOG.debug("records in first date partition are {}", recordsInDatePartition1.get(i));
      /*An array containing partition and data is returned as a string record, hence we need to upcast it to an object first
       and then downcast to a string in order to use in Assert.*/
      Object record = recordsInDatePartition1.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals("2014-12-10", records[1]);
    }
    Assert.assertEquals(7, recordsInDatePartition2.size());
    for (int i = 0; i < recordsInDatePartition2.size(); i++) {
      LOG.debug("records in second date partition are {}", recordsInDatePartition2.get(i));
      Object record = recordsInDatePartition2.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals("2014-12-11", records[1]);
    }
  }

  @Test
  public void testInsertPOJO() throws Exception
  {
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testdir);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    ArrayList<String> hiveColumns = new ArrayList<String>();
    hiveColumns.add("col1");
    hiveInitializePOJODatabase(createStore(null));
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablepojo);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    FSPojoToHiveOperator fsRolling = new FSPojoToHiveOperator();
    fsRolling.setFilePath(testdir);
    fsRolling.setHiveColumns(hiveColumns);
    ArrayList<FIELD_TYPE> fieldtypes = new ArrayList<FIELD_TYPE>();
    ArrayList<FIELD_TYPE> partitiontypes = new ArrayList<FIELD_TYPE>();
    fieldtypes.add(FIELD_TYPE.INTEGER);
    partitiontypes.add(FIELD_TYPE.STRING);
    fsRolling.setHiveColumnDataTypes(fieldtypes);
    fsRolling.setHivePartitionColumnDataTypes(partitiontypes);
    //ArrayList<FIELD_TYPE> partitionColumnType = new ArrayList<FIELD_TYPE>();
    //partitionColumnType.add(FIELD_TYPE.STRING);
    fsRolling.setHivePartitionColumns(hivePartitionColumns);
    // fsRolling.setHivePartitionColumnsDataTypes(partitionColumnType);
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("getId()");
    ArrayList<String> expressionsPartitions = new ArrayList<String>();

    expressionsPartitions.add("getDate()");
    short permission = 511;
    fsRolling.setFilePermission(permission);
    fsRolling.setAlwaysWriteToTmp(false);
    fsRolling.setMaxLength(128);
    fsRolling.setExpressionsForHiveColumns(expressions);
    fsRolling.setExpressionsForHivePartitionColumns(expressionsPartitions);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-11" + "/" + "0-transaction.out.part.0");
    ArrayList<String> partitions1 = new ArrayList<String>();
    partitions1.add("2014-12-11");
    mapping1.setPartition(partitions1);
    ArrayList<String> partitions2 = new ArrayList<String>();
    partitions2.add("2014-12-12");
    mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-12" + "/" + "0-transaction.out.part.0");
    mapping2.setPartition(partitions2);
    for (int wid = 0, total = 0; wid < NUM_WINDOWS; wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 1; tupleCounter < BLAST_SIZE && total < DATABASE_SIZE; tupleCounter++, total++) {
        InnerObj innerObj = new InnerObj();
        innerObj.setId(tupleCounter);
        innerObj.setDate("2014-12-1" + tupleCounter);
        fsRolling.input.process(innerObj);
      }
      if (wid == 7) {
        fsRolling.committed(wid - 1);
        hiveOperator.processTuple(mapping1);
        hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();

      if (wid == 6) {
        fsRolling.beforeCheckpoint(wid);
        fsRolling.checkpointed(wid);
      }
    }

    fsRolling.teardown();
    hiveStore.connect();
    client.execute("select * from " + tablepojo + " where dt='2014-12-11'");
    List<String> recordsInDatePartition1 = client.fetchAll();

    client.execute("select * from " + tablepojo + " where dt='2014-12-12'");
    List<String> recordsInDatePartition2 = client.fetchAll();
    client.execute("drop table " + tablepojo);
    hiveStore.disconnect();

    Assert.assertEquals(7, recordsInDatePartition1.size());
    for (int i = 0; i < recordsInDatePartition1.size(); i++) {
      LOG.debug("records in first date partition are {}", recordsInDatePartition1.get(i));
      /*An array containing partition and data is returned as a string record, hence we need to upcast it to an object first
       and then downcast to a string in order to use in Assert.*/
      Object record = recordsInDatePartition1.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals(1, records[0]);
      Assert.assertEquals("2014-12-11", records[1]);
    }
    Assert.assertEquals(7, recordsInDatePartition2.size());
    for (int i = 0; i < recordsInDatePartition2.size(); i++) {
      LOG.debug("records in second date partition are {}", recordsInDatePartition2.get(i));
      Object record = recordsInDatePartition2.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals(2, records[0]);
      Assert.assertEquals("2014-12-12", records[1]);
    }
  }

  @Test
  public void testHiveInsertMapOperator() throws SQLException, TException
  {
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testdir);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveInitializeMapDatabase(createStore(null));
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablemap);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    FSRollingMapTestImpl fsRolling = new FSRollingMapTestImpl();
    fsRolling.setFilePath(testdir);
    short permission = 511;
    fsRolling.setFilePermission(permission);
    fsRolling.setAlwaysWriteToTmp(false);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    HashMap<String, Object> map = new HashMap<String, Object>();
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    ArrayList<String> partitions1 = new ArrayList<String>();
    partitions1.add("2014-12-10");
    mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-10" + "/" + "0-transaction.out.part.0");
    mapping1.setPartition(partitions1);
    ArrayList<String> partitions2 = new ArrayList<String>();
    partitions2.add("2014-12-11");
    mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-11" + "/" + "0-transaction.out.part.0");
    mapping2.setPartition(partitions2);
    for (int wid = 0; wid < NUM_WINDOWS; wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0; tupleCounter < BLAST_SIZE; tupleCounter++) {
        map.put(2014 - 12 - 10 + "", 2014 - 12 - 10);
        fsRolling.input.put(map);
        map.clear();
      }

      if (wid == 7) {
        fsRolling.committed(wid - 1);
        hiveOperator.processTuple(mapping1);
        hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();
    }

    fsRolling.teardown();

    hiveStore.connect();

    client.execute("select * from " + tablemap + " where dt='2014-12-10'");
    List<String> recordsInDatePartition1 = client.fetchAll();

    client.execute("drop table " + tablemap);
    hiveStore.disconnect();

    Assert.assertEquals(13, recordsInDatePartition1.size());
    for (int i = 0; i < recordsInDatePartition1.size(); i++) {
      LOG.debug("records in first date partition are {}", recordsInDatePartition1.get(i));
      /*An array containing partition and data is returned as a string record, hence we need to upcast it to an object first
       and then downcast to a string in order to use in Assert.*/
      Object record = recordsInDatePartition1.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals("2014-12-10", records[1]);
    }

  }

  @Test
  public void testHDFSHiveCheckpoint() throws SQLException, TException
  {
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testdir);
    HiveOperator outputOperator = new HiveOperator();
    HiveOperator newOp;
    outputOperator.setStore(hiveStore);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    FSRollingTestImpl fsRolling = new FSRollingTestImpl();
    hiveInitializeDatabase(createStore(null));
    outputOperator.setHivePartitionColumns(hivePartitionColumns);
    outputOperator.setTablename(tablename);
    fsRolling.setFilePath(testdir);
    short persmission = 511;
    fsRolling.setFilePermission(persmission);
    fsRolling.setAlwaysWriteToTmp(false);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);

    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    FilePartitionMapping mapping3 = new FilePartitionMapping();
    outputOperator.setup(context);
    mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-10" + "/" + "0-transaction.out.part.0");
    ArrayList<String> partitions1 = new ArrayList<String>();
    partitions1.add("2014-12-10");
    mapping1.setPartition(partitions1);
    mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-11" + "/" + "0-transaction.out.part.0");
    ArrayList<String> partitions2 = new ArrayList<String>();
    partitions2.add("2014-12-11");
    mapping2.setPartition(partitions2);
    ArrayList<String> partitions3 = new ArrayList<String>();
    partitions3.add("2014-12-12");
    mapping3.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-12" + "/" + "0-transaction.out.part.0");
    mapping3.setPartition(partitions3);
    for (int wid = 0, total = 0; wid < NUM_WINDOWS; wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0; tupleCounter < BLAST_SIZE && total < DATABASE_SIZE; tupleCounter++, total++) {
        fsRolling.input.process("2014-12-1" + tupleCounter);
      }
      if (wid == 7) {
        fsRolling.committed(wid - 1);
        outputOperator.processTuple(mapping1);
        outputOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();

      if ((wid == 6) || (wid == 9)) {
        fsRolling.beforeCheckpoint(wid);
        fsRolling.checkpointed(wid);
      }

      if (wid == 9) {
        Kryo kryo = new Kryo();
        FieldSerializer<HiveOperator> f1 = (FieldSerializer<HiveOperator>)kryo.getSerializer(HiveOperator.class);
        FieldSerializer<HiveStore> f2 = (FieldSerializer<HiveStore>)kryo.getSerializer(HiveStore.class);

        f1.setCopyTransient(false);
        f2.setCopyTransient(false);
        newOp = kryo.copy(outputOperator);
        outputOperator.teardown();
        newOp.setup(context);

        newOp.beginWindow(7);
        newOp.processTuple(mapping3);

        newOp.endWindow();
        newOp.teardown();
        break;
      }

    }

    hiveStore.connect();

    client.execute("select * from " + tablename + " where dt='2014-12-10'");
    List<String> recordsInDatePartition1 = client.fetchAll();

    client.execute("select * from " + tablename + " where dt='2014-12-11'");
    List<String> recordsInDatePartition2 = client.fetchAll();

    client.execute("select * from " + tablename + " where dt='2014-12-12'");
    List<String> recordsInDatePartition3 = client.fetchAll();

    client.execute("drop table " + tablename);
    hiveStore.disconnect();

    Assert.assertEquals(7, recordsInDatePartition1.size());
    for (int i = 0; i < recordsInDatePartition1.size(); i++) {
      LOG.debug("records in first date partition are {}", recordsInDatePartition1.get(i));
      /*An array containing partition and data is returned as a string record, hence we need to upcast it to an object first
       and then downcast to a string in order to use in Assert.*/
      Object record = recordsInDatePartition1.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals("2014-12-10", records[1]);
    }
    Assert.assertEquals(7, recordsInDatePartition2.size());
    for (int i = 0; i < recordsInDatePartition2.size(); i++) {
      LOG.debug("records in second date partition are {}", recordsInDatePartition2.get(i));
      Object record = recordsInDatePartition2.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals("2014-12-11", records[1]);
    }
    Assert.assertEquals(10, recordsInDatePartition3.size());
    for (int i = 0; i < recordsInDatePartition3.size(); i++) {
      LOG.debug("records in second date partition are {}", recordsInDatePartition3.get(i));
      Object record = recordsInDatePartition3.get(i);
      Object[] records = (Object[])record;
      Assert.assertEquals("2014-12-12", records[1]);
    }

  }

  public class InnerObj
  {
    public InnerObj()
    {
    }

    private int id;
    private String date;

    public String getDate()
    {
      return date;
    }

    public void setDate(String date)
    {
      this.date = date;
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(HiveMockTest.class);

}
