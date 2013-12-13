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
package com.datatorrent.lib.parser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import static org.junit.Assert.*;

import org.junit.Test;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ift.CellProcessor;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

/**
 * <p>Unit test for CsvParserOperator</p>
 *
 * @since 0.9.2
 */
public class CsvParserOperatorTest
{
  
  static String[] header = new String[]{"name", "accessible", "birth"};
  @Test
  public void testOperator() throws ParseException
  {
    CollectorTestSink<Object> mapSink = new CollectorTestSink<Object>();
    CsvParserOperator oper = new CsvParserOperator();
    oper.mapOutput.setSink(mapSink);
    oper.setHeaderMapping(new TestHeaderMapping());
    
    oper.setup(null);
    
    oper.beginWindow(0);
    
    String testCsvString = "\"Doctor Who\",\"true\",\"1983-11-06\"\n \"Doctor WhoII\",\"FALSE\",\"1966-10-20\"";
    
    oper.stringInput.process(testCsvString);
    
    oper.endWindow();
    @SuppressWarnings("unchecked")
    Map<String, Object> collectedTuple = (Map<String, Object>) mapSink.collectedTuples.get(0);
    assertTrue("The ", collectedTuple.keySet().equals(Sets.newHashSet(header)));
    Object[] expectedVal = new Object[]{"Doctor Who", Boolean.TRUE, new SimpleDateFormat("yyyy-MM-dd").parse("1983-11-06")};
    int j = 0;
    for (String k : header) {
      assertTrue("Expected: " + expectedVal[j] + ",  Actual:" + collectedTuple.get(k).toString(), collectedTuple.get(k).equals(expectedVal[j++]));
    }
  }
  

}

class TestHeaderMapping implements CSVHeaderMapping{

  @Override
  public CellProcessor[] getProcessors()
  {
    return new CellProcessor[]{new Optional(), new ParseBool(), new ParseDate("yyyy-MM-dd")};
  }

  @Override
  public String[] getHeaders()
  {
    return CsvParserOperatorTest.header;
  }
  
}