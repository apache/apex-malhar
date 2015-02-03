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

import com.datatorrent.contrib.hive.FSRollingOutputOperator.FilePartitionMapping;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;


public class HiveMockTest extends HiveTestService
{
  private static transient final String filepath = "file:///testing";
  public static final String APP_ID = "HiveOperatorTest";
  public static final int OPERATOR_ID = 0;
  public static final int NUM_WINDOWS = 10;
  public static final int BLAST_SIZE = 10;
  public static final int DATABASE_SIZE = NUM_WINDOWS * BLAST_SIZE;
  public static final String tablename = "HiveTest";
  public static final String tablemap = "tempmaptable";
  public static String delimiterMap = ":";


  public HiveMockTest() throws IOException
  {
    super();
  }

  @Test
  public void testExecute() throws Exception
  {
    ArrayList<String> hivePartitionColumns = new ArrayList<String>();
    hivePartitionColumns.add("dt");
    hivePartitionColumns.add("country");
    client.execute("drop table " + tablename);
    client.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (col1 string) PARTITIONED BY(dt STRING,country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\n'  \n"
            + "STORED AS TEXTFILE ");
    //Path p = new Path(this.ROOT_DIR, "afile");
    HiveStore hiveStore = new HiveStore();
    hiveStore.setFilepath(filepath);
    HiveOperator hiveOperator = new HiveOperator();
    hiveOperator.setStore(hiveStore);
    hiveOperator.setTablename(tablename);
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);

    FSRollingTestImpl fsRolling = new FSRollingTestImpl();
    fsRolling.setFilePath(filepath);
    fsRolling.setFilePermission(0777);
    fsRolling.setMaxLength(128);
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);
    fsRolling.setup(context);
    hiveOperator.fs = hiveOperator.getHDFSInstance();
    fsRolling.setConverter(new StringConverter());
    FilePartitionMapping mapping1 = new FilePartitionMapping();
    String command = null;
    FilePartitionMapping mapping2 = new FilePartitionMapping();
    for (int wid = 0, total = 0;
            wid < NUM_WINDOWS;
            wid++) {
      fsRolling.beginWindow(wid);
      for (int tupleCounter = 0;
              tupleCounter < BLAST_SIZE && total < DATABASE_SIZE;
              tupleCounter++, total++) {
        fsRolling.input.put(111 + "");
        fsRolling.input.put(222 + "");
      }
      if (wid == 7) {
        fsRolling.committed(wid - 1);
        mapping1.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "222" + "/" + "0-transaction.out.part.0");

        ArrayList<String> partitions = new ArrayList<String>();
        partitions.add("111");
        partitions.add("222");
        mapping1.setPartition(partitions);
        command = hiveOperator.processHiveFile(mapping1);
        client.execute(command);
        mapping2.setFilename(APP_ID + "/" + OPERATOR_ID + "/" + "111" + "/" + "222" + "/" + "0-transaction.out.part.1");
        mapping2.setPartition(partitions);
        command = hiveOperator.processHiveFile(mapping2);
        client.execute(command);
      }

      fsRolling.endWindow();

    }

    /* FSDataOutputStream o = this.getFileSystem().create(p);
     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
     bw.write("abc\n");
     bw.write("prer\n");
     bw.write("you\n");
     bw.close();
     client.execute("load data local inpath '" + p.toString() + "' into table atest");*/
    client.execute(command);
    client.execute("select * from " + tablename);
    List<String> row = client.fetchAll();

    Assert.assertEquals(99, row.size());
  }

}
