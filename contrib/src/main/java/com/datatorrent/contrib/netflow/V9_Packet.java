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
package com.datatorrent.contrib.netflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Descriptions of the fields are given at 
 * http://www.cisco.com/en/US/technologies/tk648/tk362/technologies_white_paper09186a00800a3db9.pdf
 * 
 * Bytes |Contents       |
 * 0-1   |version        | 
 * 2-3   |count          |
 * 4-7   |SysUptime      |
 * 8-11  |unix_secs      |
 * 12-15 |PackageSequence|
 * 16-19 |Source ID      | 
 *
 * This has the logic to parse the netflow v9 packet
 */
public class V9_Packet implements Packet
{
  long version, count, SysUptime, unix_secs, packageSequence, sourceId;
  private String routerIP;
  private List<Map<Integer,String>> list;
  public static final int V9_Header_Size = 20;

  /**
   * 
   * @param RouterIP
   * @param buf
   * @param len
   * @throws DoneException
   */
  public V9_Packet(String RouterIP, byte[] buf, int len) throws Exception
  {
    if (len < V9_Header_Size) {
      throw new Exception("    * incomplete header *");
    }
    int offset = 0;
    this.routerIP = RouterIP;
    version = Util.to_number(buf, offset, 2);
    offset += 2;
    count = Util.to_number(buf, offset, 2);
    offset += 2;
    SysUptime = Util.to_number(buf, offset, 4);
    offset += 4;
    unix_secs = Util.to_number(buf, offset, 4);
    offset += 4;
    packageSequence = Util.to_number(buf, offset, 4);
    offset += 4;
    sourceId = Util.to_number(buf, 16, 4);
    offset += 4;

    list = new ArrayList<Map<Integer,String>>();
    long flowsetLength = 0l;
    for (int flowsetCounter = 0, packetOffset = offset; flowsetCounter < count && packetOffset < len; flowsetCounter++, packetOffset += flowsetLength) {
      int flowsetId = (int)Util.to_number(buf, packetOffset, 2);
      flowsetLength = Util.to_number(buf, packetOffset + 2, 2);
      if (flowsetLength == 0) {
        throw new Exception("there is a flowset len=0");
      }
      if (flowsetId == 0) {
        // template flowset arrived        
        int templateOffset = packetOffset + 4;        
        do {
          long fieldCount = Util.to_number(buf, templateOffset + 2, 2);
          TemplateManager.getTemplateManager().acceptTemplate(this.routerIP, buf, templateOffset);
          templateOffset += fieldCount * 4 + 4; // 4 is added for off-setting template id and field count
        } while (templateOffset - packetOffset < flowsetLength);
      } else if (flowsetId == 1) { // options flowset
        continue;       
      } else if (flowsetId > 255) {
        // data flowset
        // templateId==flowsetId
        Template tOfData = TemplateManager.getTemplateManager().getTemplate(this.routerIP,flowsetId); 
        if (tOfData != null) { 
          Map<Integer,Template.OffsetLengthObj> fieldTypeMap = tOfData.getFieldTypeMap();
          int dataTotalLength = tOfData.getTotalLength();
          for (int p = packetOffset + 4; (p - packetOffset + dataTotalLength) < flowsetLength; ) {
            Map<Integer, String> dataMap = new HashMap<Integer, String>();
            for(Map.Entry<Integer, Template.OffsetLengthObj> entry: fieldTypeMap.entrySet()){
              dataMap.put(entry.getKey(), Util.toString(buf, entry.getValue().offset, entry.getValue().length));
            }
            list.add(dataMap);            
            p += dataTotalLength;
          }
        }
      } else { // options packet, should refer to option template, not in use now
        continue;         
      }
    }
  }
}
