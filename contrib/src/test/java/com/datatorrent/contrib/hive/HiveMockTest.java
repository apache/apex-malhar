package com.datatorrent.contrib.hive;

import io.teknek.hiveunit.HiveTestService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import static com.datatorrent.contrib.hive.AbstractHiveOutputOperatorTest.*;
import com.datatorrent.lib.util.TestUtils.TestInfo;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.jdbc.HiveStatement;
import org.apache.thrift.TException;
import org.junit.Rule;
import org.junit.runner.Description;


public class HiveMockTest extends HiveTestService
{
  public static final String APP_ID = "HiveOperatorTest";

  public static final int OPERATOR_ID = 0;
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final String tablename = "temptable";
  public static final String tablemap = "tempmap";
  public static String delimiterMap = ":";


  @Rule
  public TestInfo testMeta = new HiveTestWatcher();

  public static class HiveTestWatcher extends TestInfo
  {
    @Override
    public String getDir()
    {
      String filePath = new File("target/").getAbsolutePath();
      LOG.debug("filepath is {}", filePath);
      return filePath;
    }

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }

  }

  public HiveMockTest() throws IOException, Exception
  {
    super();
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
    //hiveStore.setDbUrl(tempHost);

    sb.append("user:").append("").append(",");
    sb.append("password:").append("");

    String properties = sb.toString();
    LOG.debug(properties);
    hiveStore.setDbDriver("org.apache.hive.jdbc.HiveDriver");

    hiveStore.setDbUrl("jdbc:hive2://");
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
      LOG.debug("tables are {}",res.getString(1));
    }

    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (col1 string) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    /*ResultSet res = stmt.execute("CREATE TABLE IF NOT EXISTS temp4 (col1 map<string,int>,col2 map<string,int>,col3  map<string,int>,col4 map<String,timestamp>, col5 map<string,double>,col6 map<string,double>,col7 map<string,int>,col8 map<string,int>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  \n"
     + "COLLECTION ITEMS TERMINATED BY '\n'  \n"
     + "MAP KEYS TERMINATED BY ':'  \n"
     + "LINES TERMINATED BY '\n' "
     + "STORED AS TEXTFILE");*/

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

    stmt.execute("CREATE TABLE IF NOT EXISTS " + tablemap + " (col1 map<string,int>) PARTITIONED BY(dt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "MAP KEYS TERMINATED BY '" + delimiterMap + "' \n"
            + "STORED AS TEXTFILE ");
    hiveStore.disconnect();
  }

  @Override
  public void setUp() throws Exception
  {
    System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.toString(),testMeta.getDir());
    super.setUp();
  }

  @Test
  public void testInsertString() throws Exception
  {
    HiveStore hiveStore = createStore(null);
    HiveStreamCodec<String> codec = new HiveStreamCodec<String>();
    hiveStore.setFilepath(testMeta.getDir());
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveInitializeDatabase(createStore(null));
    //Path p = new Path(this.ROOT_DIR, "afile");
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablename);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    FSRollingTestImpl fsRolling = new FSRollingTestImpl();
    fsRolling.setFilePath(testMeta.getDir());
    fsRolling.setFilePermission(511);
    fsRolling.setMaxLength(128);
    fsRolling.setStreamCodec(codec);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
     mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-10" + "/"+ "0-transaction.out.part.0");
        ArrayList<String> partitions = new ArrayList<String>();
        partitions.add("2014-12-10");
        partitions.add("2014-12-11");
         mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "2014-12-11" + "/" + "0-transaction.out.part.1");
        mapping2.setPartition(partitions);
        mapping1.setPartition(partitions);
    for (int wid = 0, total = 0;
            wid < NUM_WINDOWS;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
              tupleCounter++, total++) {
        fsRolling.input.process("2014-12-1"+tupleCounter);
      }
      if (wid == 7) {
        fsRolling.committed(wid - 1);
        hiveOperator.processTuple(mapping1);
        hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();
     // hiveOperator.endWindow();
    }

    fsRolling.teardown();
    hiveStore.connect();
    client.execute("select * from " + tablename + " where dt='2014-12-10'");
   // List<String> records = client.fetchAll();
    Statement stmt = hiveStore.getConnection().createStatement();
   // ResultSet resultSet = stmt.execute("select * from " + tablename + " where dt='2014-12-10'");
   // resultSet.next();
   // int databaseSize = resultSet.getInt(1);
   // LOG.debug("records in date partition {}", databaseSize);
    List<String> recordsInDatePartition = client.fetchAll();
    for(int i=0;i<recordsInDatePartition.size();i++)
    LOG.debug("records in date partition {}", recordsInDatePartition.get(i));

  //  String[] numPartitions = partitions.split(",");

     stmt.execute("drop table " + tablename);
   // resultSet.next();
   // databaseSize = resultSet.getInt(1);
   // LOG.debug("database size is {}", databaseSize);
    hiveStore.disconnect();

   // Assert.assertEquals(66, records.size());
   // Assert.assertEquals(2, numPartitions.length);

  }


  @Test
  public void testHiveInsertMapOperator() throws SQLException, TException
  {
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hiveInitializeMapDatabase(createStore(null));


    //Path p = new Path(this.ROOT_DIR, "afile");
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablemap);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    FSRollingMapTestImpl fsRolling = new FSRollingMapTestImpl();
    fsRolling.setFilePath(testMeta.getDir());
    fsRolling.setFilePermission(511);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);
    hiveOperator.setup(context);
    HashMap<String, Object> map = new HashMap<String, Object>();
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    ArrayList<String> partitions = new ArrayList<String>();
    partitions.add("111");
    for (int wid = 0;
            wid < NUM_WINDOWS;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE;
              tupleCounter++) {
        map.put(111 + "", 111);
        fsRolling.input.put(map);
        map.clear();
      }

      if (wid == 7) {
        fsRolling.committed(wid - 1);
        mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/"+ "0-transaction.out.part.0");
        mapping1.setPartition(partitions);
        hiveOperator.processTuple(mapping1);
        mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "0-transaction.out.part.1");
        mapping2.setPartition(partitions);
        hiveOperator.processTuple(mapping2);
      }

      fsRolling.endWindow();
    }

    fsRolling.teardown();

    hiveStore.connect();

    client.execute("select * from " + tablemap);
    List<String> row = client.fetchAll();
    client.execute("drop table " + tablemap);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        34,
                        row.size());

  }

  @Test
  public void testHDFSHiveCheckpoint() throws SQLException, TException
  {
    hiveInitializeDatabase(createStore(null));
    HiveStore hiveStore = createStore(null);
    hiveStore.setFilepath(testMeta.getDir());
    HiveOperator outputOperator = new HiveOperator();
    HiveOperator newOp = new HiveOperator();

    outputOperator.setStore(hiveStore);
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hivePartitionColumns.add("country");
    FSRollingTestImpl fsRolling = new FSRollingTestImpl();
    hiveInitializeDatabase(createStore(null));
    outputOperator.setHivePartitionColumns(hivePartitionColumns);
    outputOperator.setTablename(tablename);
    fsRolling.setFilePath(testMeta.getDir());
    fsRolling.setFilePermission(0511);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    fsRolling.setup(context);

    FilePartitionMapping mapping1 = new FilePartitionMapping();
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    FilePartitionMapping mapping3 = new FilePartitionMapping();
    outputOperator.setup(context);
    ArrayList<String> partitions = new ArrayList<String>();
    partitions.add("111");
    partitions.add("222");
    for (int wid = 0, total = 0;
            wid < 20;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < 10 && total < 100;
              tupleCounter++, total++) {
        fsRolling.input.process(123 + "");
      }
      if (wid == 6) {
        fsRolling.committed(wid - 1);
        mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "222" + "/"+ "0-transaction.out.part.0");
        mapping1.setPartition(partitions);
        outputOperator.processTuple(mapping1);
      }
      if (wid == 15) {
        fsRolling.committed(14);
        mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "222" + "/"+ "0-transaction.out.part.1");
        mapping2.setPartition(partitions);
        outputOperator.processTuple(mapping2);
      }

      if (wid == 18) {
        Kryo kryo = new Kryo();
        FieldSerializer<HiveOperator> f1 = (FieldSerializer<HiveOperator>)kryo.getSerializer(HiveOperator.class);
        FieldSerializer<HiveStore> f2 = (FieldSerializer<HiveStore>)kryo.getSerializer(HiveStore.class);

        f1.setCopyTransient(false);
        f2.setCopyTransient(false);
        newOp = kryo.copy(outputOperator);
        outputOperator.teardown();
        newOp.setup(context);

        newOp.beginWindow(15);
        mapping3.setFilename("0-transactions.out.part.2");
        mapping3.setPartition(partitions);

        newOp.processTuple(mapping3);

        newOp.endWindow();
        newOp.teardown();
        break;
      }

    }

    hiveStore.connect();


    client.execute("select * from " + tablename);
    List<String> row = client.fetchAll();
    client.execute("drop table " + tablename);
    hiveStore.disconnect();

    Assert.assertEquals("Numer of tuples in database",
                        66,
                        row.size());

  }

    private static transient final Logger LOG = LoggerFactory.getLogger(HiveMockTest.class);

}
