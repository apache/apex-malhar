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
import java.util.Map.Entry;

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
 * Takes a configuration string which tells us about the position of the row, or
 * column.&nbsp; The incoming tuples are inserted accordingly.
 * <p>
 *
 * @displayName HBase Csv Mapping Put
 * @category Output
 * @tags hbase, csv, put, String
 * @since 1.0.4
 */
public class HBaseCsvMappingPutOperator extends AbstractHBaseWindowPutOutputOperator<String>
{
  private class ColDef
  {
    String colFam;
    String colName;
  }

  private static final transient Logger logger = LoggerFactory.getLogger(HBaseCsvMappingPutOperator.class);
  private transient Integer rowIndex;
  private transient Map<Integer, ColDef> colMap = new HashMap<Integer, ColDef>();
  private transient ICsvListReader lineListReader = null;
  private transient ReusableStringReader lineSr = new ReusableStringReader();
  private transient ArrayList<String> csvLineList = new ArrayList<String>();
  private String mappingString;

  public void setMappingString(String mappingString)
  {
    this.mappingString = mappingString;
  }

  @Override
  public Put operationPut(String t) throws IOException
  {
    return parseLine(t);
  }

  public void parseMapping()
  {
    ICsvListReader listReader = null;
    StringReader sr = null;
    ArrayList<String> csvList = new ArrayList<String>();
    try {
      sr = new StringReader(mappingString);
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
    for (int index = 0; index < csvList.size(); index++) {
      String value = csvList.get(index);
      if (value.equals("row")) {
        rowIndex = index;
      } else {
        ColDef c = new ColDef();
        c.colFam = value.substring(0, value.indexOf('.'));
        c.colName = value.substring(value.indexOf('.') + 1);
        colMap.put(index, c);
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
    put = new Put(csvLineList.get(rowIndex).getBytes());
    for (Entry<Integer, ColDef> e : colMap.entrySet()) {
      String colValue = csvLineList.get(e.getKey());
      put.add(e.getValue().colFam.getBytes(),e.getValue().colName.getBytes(), colValue.getBytes());
    }

    csvLineList.clear();
    return put;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    parseMapping();
    lineListReader = new CsvListReader(lineSr,
    CsvPreference.STANDARD_PREFERENCE);
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
