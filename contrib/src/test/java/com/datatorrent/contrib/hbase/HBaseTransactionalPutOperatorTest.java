/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import junit.framework.Assert;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class HBaseTransactionalPutOperatorTest {
	private static final Logger logger = LoggerFactory
			.getLogger(HBaseTransactionalPutOperatorTest.class);

	public HBaseTransactionalPutOperatorTest() {
	}

	@Test
	public void testPut() {
		try {
      HBaseTestHelper.startLocalCluster();
			HBaseTestHelper.clearHBase();
			LocalMode lma = LocalMode.newInstance();
			DAG dag = lma.getDAG();

			dag.setAttribute(DAG.APPLICATION_NAME, "HBasePutOperatorTest");
			HBaseRowTupleGenerator rtg = dag.addOperator("tuplegenerator",
					HBaseRowTupleGenerator.class);
			TestHBasePutOperator thop = dag.addOperator("testhbaseput",
					TestHBasePutOperator.class);
			dag.addStream("ss", rtg.outputPort, thop.input);

			thop.getStore().setTableName("table1");
			thop.getStore().setZookeeperQuorum("127.0.0.1");
			thop.getStore().setZookeeperClientPort(2181);

			LocalMode.Controller lc = lma.getController();
			lc.setHeartbeatMonitoringEnabled(false);
			lc.run(30000);

			HBaseTuple tuple = HBaseTestHelper.getHBaseTuple("row0", "colfam0",
					"col-0");
			Assert.assertNotNull("Tuple", tuple);
			Assert.assertEquals("Tuple row", tuple.getRow(), "row0");
			Assert.assertEquals("Tuple column family", tuple.getColFamily(),
					"colfam0");
			Assert.assertEquals("Tuple column name", tuple.getColName(),
					"col-0");
			Assert.assertEquals("Tuple column value", tuple.getColValue(),
					"val-0-0");
			tuple = HBaseTestHelper.getHBaseTuple("row499", "colfam0", "col-0");
			Assert.assertNotNull("Tuple", tuple);
			Assert.assertEquals("Tuple row", tuple.getRow(), "row499");
			Assert.assertEquals("Tuple column family", tuple.getColFamily(),
					"colfam0");
			Assert.assertEquals("Tuple column name", tuple.getColName(),
					"col-0");
			Assert.assertEquals("Tuple column value", tuple.getColValue(),
					"val-499-0");
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.getMessage());
			assert false;
		}
	}

	@SuppressWarnings("serial")
	public static class TestHBasePutOperator extends
			AbstractHBaseTransactionalPutOutputOperator<HBaseTuple> {

		@Override
		public Put operationPut(HBaseTuple t) throws IOException {
			Put put = new Put(t.getRow().getBytes());
			put.add(t.getColFamily().getBytes(), t.getColName().getBytes(), t
					.getColValue().getBytes());
			return put;
		}

	}
}
