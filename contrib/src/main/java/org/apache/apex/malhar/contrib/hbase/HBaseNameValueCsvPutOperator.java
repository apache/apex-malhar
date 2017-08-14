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
package org.apache.apex.malhar.contrib.hbase;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.apache.apex.malhar.lib.util.ReusableStringReader;
import org.apache.hadoop.hbase.client.Put;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * An implementation of HBase window put operator that inserts a string of key
 * value pairs which are mapped to corresponding rows, columns.
 * <p>
 * Accepts a string of key value pairs containing the data to be inserted.These
 * are mapped to corresponding rows,column families and columns using a property
 * file and inserted into hbase Example: input string will be of the form
 * name="xyz", st="patrickhenry" ,ct="fremont", sa="california", the properties
 * will contain properties of form name=row, sa=address.street, ct=address.city,
 * sa=address.state. With the above mapping a row xyz is created. The value
 * patrickhenry is inserted into columnfamily address and column street of row
 * xyz. Other values are inserted similarly.
 *
 * @displayName HBase Name Value Csv Put
 * @category Output
 * @tags csv, hbase, put
 * @since 1.0.2
 */
public class HBaseNameValueCsvPutOperator extends AbstractHBaseWindowPutOutputOperator<String>
{
  private class ColDef
  {
    String colFam;
    String colName;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(HBaseNameValueCsvPutOperator.class);
  @NotNull
  private String mapping;
  private transient String rowKey;
  private transient Map<String, ColDef> colMap = new HashMap<String, ColDef>();
  private transient Map<String, String> linemap = new HashMap<String, String>();
  private transient ICsvListReader lineListReader = null;
  private transient ReusableStringReader lineSr = new ReusableStringReader();
  private transient ArrayList<String> csvLineList = new ArrayList<String>();

  public void setMapping(String mapping)
  {
    this.mapping = mapping;
  }

  @Override
  public Put operationPut(String t)
  {
    return parseLine(t);
  }

  public void parseMapping()
  {
    ICsvListReader listReader = null;
    StringReader sr = null;
    ArrayList<String> csvList = new ArrayList<String>();
    try {
      sr = new StringReader(mapping);
      listReader = new CsvListReader(sr,CsvPreference.STANDARD_PREFERENCE);
      csvList = (ArrayList<String>)listReader.read();
    } catch (IOException e) {
      logger.error("Cannot read the mapping string", e);
      DTThrowable.rethrow(e);
    } finally {
      try {
        sr.close();
        listReader.close();
      } catch (IOException e) {
        logger.error("Error closing Csv reader", e);
        DTThrowable.rethrow(e);
      }
    }
    for (String pair : csvList) {
      ColDef c = new ColDef();
      if (pair.indexOf('.') != -1) {
        c.colName = pair.substring(pair.indexOf('.') + 1);
        c.colFam = pair.substring(pair.indexOf('=') + 1,pair.indexOf('.'));
        colMap.put(pair.substring(0, pair.indexOf('=')), c);
      } else {
        rowKey = pair.substring(0, pair.indexOf('='));
      }
    }
  }

  public Put parseLine(String s)
  {
    Put put = null;
    try {
      lineSr.open(s);
      csvLineList = (ArrayList<String>)lineListReader.read();
    } catch (IOException e) {
      logger.error("Cannot read the property string", e);
      DTThrowable.rethrow(e);
    }
    for (String pair : csvLineList) {
      String key = pair.substring(0, pair.indexOf('='));
      String value = pair.substring(pair.indexOf('=') + 1);

      if (key.equals(rowKey)) {
        put = new Put(value.getBytes());
        for (Map.Entry<String, String> entry : linemap.entrySet()) {
          ColDef c = colMap.get(entry.getKey());
          put.add(c.colFam.getBytes(), c.colName.getBytes(), entry.getValue().getBytes());
        }
      } else {
        if (put != null) {
          ColDef c = colMap.get(key);
          put.add(c.colFam.getBytes(), c.colName.getBytes(),value.getBytes());
        } else {
          linemap.put(key, value);
        }
      }
    }
    csvLineList.clear();
    linemap.clear();
    return put;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    parseMapping();
    lineListReader = new CsvListReader(lineSr,CsvPreference.STANDARD_PREFERENCE);
  }

  @Override
  public void teardown()
  {
    super.teardown();
    try {
      lineSr.close();
      lineListReader.close();
    } catch (IOException e) {
      logger.error("Cannot close the readers", e);
      DTThrowable.rethrow(e);
    }
  }

}
