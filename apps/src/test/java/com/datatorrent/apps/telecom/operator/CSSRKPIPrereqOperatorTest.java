package com.datatorrent.apps.telecom.operator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.testbench.CollectorTestSink;

public class CSSRKPIPrereqOperatorTest
{

  @Test
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testOperator()
  {
    CSSRKPIPrereqOperator oper = new CSSRKPIPrereqOperator();
    oper.setTimeField("time");
    List<String> fieldsToCalculateKPI = new ArrayList<String>();
    fieldsToCalculateKPI.add("success");
    fieldsToCalculateKPI.add("channel");
    
    oper.setFieldsToCalculateKPI(fieldsToCalculateKPI);
    oper.setDateFormat("yy/MM/dd HH:mm:ss");

    DateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.output.setSink(sortSink);

    oper.setup(null);
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 0);
    oper.beginWindow(0);
    for (int i = 0; i < 100; i++) {
      Map<String,Object> inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("success", 1);
      oper.input.process(inputMap);
      inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("channel", 1);
      oper.input.process(inputMap);
    }
    oper.endWindow();
        
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 1);
    oper.beginWindow(1);
    for (int i = 0; i < 10; i++) {
      Map<String,Object> inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("success", 1);
      oper.input.process(inputMap);
      inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("channel", 1);
      oper.input.process(inputMap);
    }
    oper.endWindow();
    
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 0);
    oper.beginWindow(2);
    for (int i = 0; i < 10; i++) {
      Map<String,Object> inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("success", 1);
      oper.input.process(inputMap);
      inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("channel", 1);
      oper.input.process(inputMap);
    }
    oper.endWindow();
    
    cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, 2);
    oper.beginWindow(3);
    for (int i = 0; i < 10; i++) {
      Map<String,Object> inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("success", 1);
      oper.input.process(inputMap);
      inputMap = new HashMap<String, Object>();
      inputMap.put("time", format.format(cal.getTime()));
      inputMap.put("channel", 1);
      oper.input.process(inputMap);
    }
    oper.endWindow();
    
    for (Object o : sortSink.collectedTuples) {
      System.out.println(o.toString());
      logger.info(o.toString());
    }
  }

  private static Logger logger = LoggerFactory.getLogger(CSSRKPIPrereqOperatorTest.class);
}
