package com.datatorrent.apps.telecom.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class CDRGenerator
{

  private static String[] callType = {"V","VOIP","D","C","N","I","U","B","X","M","G"};
  private static String[] callCause = {"0","1"};
  private static String[] timeBand = {"Peak","OffPeak","Weekend","Special1"};
  private static String[] recording = {"1","0",""};
  private static String[] vat = {"S","Z"};
  
  private static int callTypeIndex;
  private static int callCauseIndex;
  private static int timeBandIndex;
  private static int recordingIndex;
  private static int vatIndex;
  
  private static DateFormat callDate = new SimpleDateFormat("dd/MM/yyyy");
  private static DateFormat callTime = new SimpleDateFormat("HH:mm:ss");
  
  public static String getNextCDR(){
    StringBuilder builder = new StringBuilder();
    builder.append(callType[callTypeIndex]);
    String currentCallType = callType[callTypeIndex];
    callTypeIndex = (callTypeIndex + 1) % callType.length;
    
    builder.append(","+callCause[callCauseIndex]);
    callCauseIndex = (callCauseIndex +1) % callCause.length;
    
    builder.append(",0"+RandomString.nextString(10));
    builder.append(",0"+RandomString.nextString(10));
    
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    builder.append(","+callDate.format(date));
    builder.append(","+callTime.format(date));
    builder.append(","+RandomString.nextString(3));
    if(currentCallType.equalsIgnoreCase("G")){
      builder.append(","+RandomString.nextString(3));
      builder.append(","+RandomString.nextString(3));
    }else{
      builder.append(",,");
    }
    builder.append(",description");
    builder.append(",charge code");
    builder.append(","+timeBand[timeBandIndex]);
    timeBandIndex =(timeBandIndex +1)% timeBand.length;
    
    return builder.toString();
  }
  
  public static void main(String[] args){
    System.out.println(CDRGenerator.getNextCDR());
    System.out.println(CDRGenerator.getNextCDR());
  }
}
