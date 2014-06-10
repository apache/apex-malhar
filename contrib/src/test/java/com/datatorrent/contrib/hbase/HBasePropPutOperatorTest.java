package com.datatorrent.contrib.hbase;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBasePropPutOperatorTest {
	private static final Logger logger = LoggerFactory.getLogger(HBasePutOperatorTest.class);
	@Test
	public void testPut()
	{
		try {
			HBaseTestHelper.clearHBase();
			HBasePropPutOperator propPutOperator=new HBasePropPutOperator();
			
			propPutOperator.setTableName("table1");
			propPutOperator.setZookeeperQuorum("127.0.0.1");
			propPutOperator.setZookeeperClientPort(2181);
			String s="name=milind,st=patrick,ct=fremont,sa=cali";
			propPutOperator.setPropFilepath("employee.properties");
			propPutOperator.setup(null);
			propPutOperator.beginWindow(0);
			propPutOperator.inputPort.process(s);
			propPutOperator.endWindow();
			HBaseTuple tuple;

			tuple = HBaseTestHelper.getHBaseTuple("milind", "colfam0", "street");

			Assert.assertNotNull("Tuple", tuple);
			Assert.assertEquals("Tuple row", tuple.getRow(), "milind");
			Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
			Assert.assertEquals("Tuple column name", tuple.getColName(), "street");
			Assert.assertEquals("Tuple column value", tuple.getColValue(), "patrick");
		} catch (IOException e) {

			logger.error(e.getMessage());
		}
	}
}
