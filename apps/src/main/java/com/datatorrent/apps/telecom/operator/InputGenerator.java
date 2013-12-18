package com.datatorrent.apps.telecom.operator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class InputGenerator implements InputOperator
{

  public transient DefaultOutputPort<Map<String,String>> output = new DefaultOutputPort<Map<String,String>>();
  /**
   * Number of tuples to be emitted in each emitTuple Call
   */
  private int tupleBlast = 1000;
  
  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void emitTuples()
  {
    for(int i = 0; i < tupleBlast; i++){
      output.emit(CDRGenerator.getNextCDR());
    }

  }

  public int getTupleBlast()
  {
    return tupleBlast;
  }

  public void setTupleBlast(int tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  public static class CDRGenerator
  {

    private static final String[] callType = { "V", "VOIP", "D", "C", "N", "I", "U", "B", "X", "M", "G" };
    private static String[] callCause = { "0", "1" };
    private static String[] timeBand = { "Peak", "OffPeak", "Weekend", "Special1" };
    private static String[] recording = { "1", "0", "" };
    private static String[] vat = { "S", "Z" };

    private static int callTypeIndex;
    private static int callCauseIndex;
    private static int timeBandIndex;
    private static int recordingIndex;
    private static int vatIndex;

    private static DateFormat callDate = new SimpleDateFormat("dd/MM/yyyy");
    private static DateFormat callTime = new SimpleDateFormat("HH:mm:ss");

    public static Map<String,String> getNextCDR()
    {
      Map<String,String> map = new HashMap<String, String>();
      map.put("callType",callType[callTypeIndex]); // call type #1
      String currentCallType = callType[callTypeIndex];
      callTypeIndex = (callTypeIndex + 1) % callType.length;

      map.put("callCause" , callCause[callCauseIndex]); // call cause definition #2
      callCauseIndex = (callCauseIndex + 1) % callCause.length;

      map.put("cli","0" + RandomStringUtils.randomNumeric(10)); // customer identifier #3
      map.put("telephone","0" + RandomStringUtils.randomNumeric(10)); // telephone number #4

      Calendar calendar = Calendar.getInstance();
      Date date = calendar.getTime();
      map.put("callDate", callDate.format(date)); // call date #5
      map.put("callTime" , callTime.format(date)); // call time #6
      map.put("duration" , RandomStringUtils.randomNumeric(3)); // duration #7
      if (currentCallType.equalsIgnoreCase("G")) {
        map.put("bytesTransmitted" , RandomStringUtils.randomNumeric(3)); // bytes transmitted #8
        map.put("bytesReceived", RandomStringUtils.randomNumeric(3)); // bytes received #9
      } 
      map.put("description","description"); // description #10
      map.put("chargeCode","charge code"); // charge code #11
      map.put("timeBand" , timeBand[timeBandIndex]); // time band #12
      timeBandIndex = (timeBandIndex + 1) % timeBand.length;
      map.put("salesPrice" , RandomStringUtils.randomNumeric(2)); // sales price #13
      map.put("preBundle" , RandomStringUtils.randomNumeric(2)); // sales price #14
      map.put("extension" , RandomStringUtils.randomNumeric(4)); // extension #15
      map.put("ddi" , RandomStringUtils.randomNumeric(10)); // ddi #16
      map.put("groupingId","1"); // grouping id #17
      map.put("recording" , recording[recordingIndex]); // recording #20
      recordingIndex = (recordingIndex + 1) % recording.length;
      map.put("vat" , vat[vatIndex]); // recording #21
      vatIndex = (vatIndex + 1) % vat.length;
      map.put("countryOfOrigin","GBR"); // country of origin #22
      if (currentCallType.equalsIgnoreCase("m")) {
        map.put("network","O2"); // network #23
      } else 
      map.put("ringTime" , RandomStringUtils.randomNumeric(2)); // ring time #28
      map.put("recordId" , RandomStringUtils.randomNumeric(4)); // record id #29
      return map;
    }

    
  }
}
