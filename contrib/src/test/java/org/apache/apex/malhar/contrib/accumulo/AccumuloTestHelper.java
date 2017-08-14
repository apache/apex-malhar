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
package org.apache.apex.malhar.contrib.accumulo;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.datatorrent.netlet.util.DTThrowable;


public class AccumuloTestHelper
{
  static Connector con;
  public static final byte[] colfam0_bytes = "colfam0".getBytes();
  public static final byte[] col0_bytes = "col-0".getBytes();
  private static final Logger logger = LoggerFactory.getLogger(AccumuloTestHelper.class);
  public static void createTable()
  {
    TableOperations tableoper = con.tableOperations();
    if (!tableoper.exists("tab1")) {
      try {
        tableoper.create("tab1");
      } catch (Exception e) {
        logger.error("error in test helper");
        DTThrowable.rethrow(e);
      }

    }
  }

  public static void clearTable()
  {
    TableOperations tableoper = con.tableOperations();
    if (!tableoper.exists("tab1")) {
      try {
        tableoper.create("tab1");
      } catch (Exception e) {
        logger.error("error in test helper");
        DTThrowable.rethrow(e);
      }
    }
    try {
      tableoper.deleteRows("tab1", null, null);
    } catch (AccumuloException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    } catch (AccumuloSecurityException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    } catch (TableNotFoundException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    }
  }

  public static void populateAccumulo() throws IOException
  {
    BatchWriterConfig config = new BatchWriterConfig();
    BatchWriter batchwriter = null;
    try {
      batchwriter = con.createBatchWriter("tab1", config);
    } catch (TableNotFoundException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    }
    try {
      for (int i = 0; i < 500; ++i) {
        String rowstr = "row" + i;
        Mutation mutation = new Mutation(rowstr.getBytes());
        for (int j = 0; j < 500; ++j) {
          String colstr = "col" + "-" + j;
          String valstr = "val" + "-" + i + "-" + j;
          mutation.put(colfam0_bytes,colstr.getBytes(),
              System.currentTimeMillis(),
              valstr.getBytes());
        }
        batchwriter.addMutation(mutation);
      }
      batchwriter.close();
    } catch (MutationsRejectedException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    }
  }

  public static AccumuloTuple findTuple(List<AccumuloTuple> tuples,
      String row, String colFamily, String colName)
  {
    AccumuloTuple mtuple = null;
    for (AccumuloTuple tuple : tuples) {
      if (tuple.getRow().equals(row)
          && tuple.getColFamily().equals(colFamily)
          && tuple.getColName().equals(colName)) {
        mtuple = tuple;
        break;
      }
    }
    return mtuple;
  }

  public static void deleteTable()
  {
    TableOperations tableoper = con.tableOperations();
    if (tableoper.exists("tab1")) {

      try {
        tableoper.delete("tab1");
      } catch (AccumuloException e) {
        logger.error("error in test helper");
        DTThrowable.rethrow(e);
      } catch (AccumuloSecurityException e) {
        logger.error("error in test helper");
        DTThrowable.rethrow(e);
      } catch (TableNotFoundException e) {
        logger.error("error in test helper");
        DTThrowable.rethrow(e);
      }

    }
  }

  public static void getConnector()
  {
    Instance instance = new ZooKeeperInstance("instance", "127.0.0.1");
    try {
      con = instance.getConnector("root", "pass");
    } catch (AccumuloException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    } catch (AccumuloSecurityException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    }

  }

  public static AccumuloTuple getAccumuloTuple(String row, String colFam,
      String colName)
  {
    Authorizations auths = new Authorizations();

    Scanner scan = null;
    try {
      scan = con.createScanner("tab1", auths);
    } catch (TableNotFoundException e) {
      logger.error("error in test helper");
      DTThrowable.rethrow(e);
    }

    scan.setRange(new Range(new Text(row)));
    scan.fetchColumn(new Text(colFam), new Text(colName));
    // assuming only one row
    for (Entry<Key, Value> entry : scan) {
      AccumuloTuple tuple = new AccumuloTuple();
      tuple.setRow(entry.getKey().getRow().toString());
      tuple.setColFamily(entry.getKey().getColumnFamily().toString());
      tuple.setColName(entry.getKey().getColumnQualifier().toString());
      tuple.setColValue(entry.getValue().toString());
      return tuple;
    }
    return null;
  }
}
