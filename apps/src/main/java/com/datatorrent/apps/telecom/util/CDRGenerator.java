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
package com.datatorrent.apps.telecom.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;

import com.datatorrent.contrib.netflow.KafkaOutput;

/**
 *  @since 0.9.2
 * @author gaurav gupta <gaurav@datatorrent.com>
 *
 */
public class CDRGenerator
{

  private static String[] callType = { "V", "VOIP", "D", "C", "N", "I", "U", "B", "X", "M", "G" };
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

  public static String getNextCDR()
  {
    StringBuilder builder = new StringBuilder();
    builder.append(callType[callTypeIndex]); // call type #1
    String currentCallType = callType[callTypeIndex];
    callTypeIndex = (callTypeIndex + 1) % callType.length;

    builder.append("," + callCause[callCauseIndex]); // call cause definition #2
    callCauseIndex = (callCauseIndex + 1) % callCause.length;

    builder.append(",0" + RandomStringUtils.randomNumeric(10)); // customer identifier #3
    builder.append(",0" + RandomStringUtils.randomNumeric(10)); // telephone number #4

    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    builder.append("," + callDate.format(date)); // call date #5
    builder.append("," + callTime.format(date)); // call time #6
    builder.append("," + RandomStringUtils.randomNumeric(3)); // duration #7
    if (currentCallType.equalsIgnoreCase("G")) {
      builder.append("," + RandomStringUtils.randomNumeric(3)); // bytes transmitted #8
      builder.append("," + RandomStringUtils.randomNumeric(3)); // bytes received #9
    } else {
      builder.append(",,");
    }
    builder.append(",description"); // description #10
    builder.append(",charge code"); // charge code #11
    builder.append("," + timeBand[timeBandIndex]); // time band #12
    timeBandIndex = (timeBandIndex + 1) % timeBand.length;
    builder.append("," + RandomStringUtils.randomNumeric(2)); // sales price #13
    builder.append("," + RandomStringUtils.randomNumeric(2)); // sales price #14
    builder.append("," + RandomStringUtils.randomNumeric(4)); // extension #15
    builder.append("," + RandomStringUtils.randomNumeric(10)); // ddi #16
    builder.append(","); // grouping id #17
    builder.append(","); // call class #18
    builder.append(","); // carrier #19
    builder.append("," + recording[recordingIndex]); // recording #20
    recordingIndex = (recordingIndex + 1) % recording.length;
    builder.append("," + vat[vatIndex]); // recording #21
    vatIndex = (vatIndex + 1) % vat.length;
    builder.append(",GBR"); // country of origin #22
    if (currentCallType.equalsIgnoreCase("m")) {
      builder.append(",O2"); // network #23
    } else {
      builder.append(","); // network #23
    }
    builder.append(","); // retail tariff #24
    builder.append(","); // remote network #25
    builder.append(","); // APN #26
    builder.append(","); // diverted number #27
    builder.append("," + RandomStringUtils.randomNumeric(2)); // ring time #28
    builder.append("," + RandomStringUtils.randomNumeric(4)); // record id #29
    return builder.toString();
  }

  public static void main(String[] args)
  {
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", args[0]);
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "200");
    props.setProperty("queue.buffering.max.messages", "10");
    props.setProperty("batch.num.messages", "5");    
    KafkaOutput<String> k = new KafkaOutput<String>(props);
    k.setTopic(args[1]);
    int i = 100000000;
    while (i>0) {
      k.process(CDRGenerator.getNextCDR());
      i--;
    }
    k.close();
  }
}
