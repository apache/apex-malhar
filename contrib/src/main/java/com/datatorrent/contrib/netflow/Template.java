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

import java.util.HashMap;
import java.util.Map;

/**
 * This is encapsulates the template definition
 *
 * @since 0.9.2
 */
public class Template
{
  private int MAX_FIELD_TYPE = 96;
  private Map<Integer, OffsetLengthObj> fieldTypeMap;

  private int tid;
  private String routerIp = null;
  private int totalLength;

  /**
   * 
   * @param routerIp
   * @param flowset
   * @param templateOffset
   * 
   * @throws Exception
   */
  public void createTemplate(String routerIp, byte[] flowset, int templateOffset) throws Exception
  {
    totalLength = 0;
    fieldTypeMap = new HashMap<Integer, OffsetLengthObj>();
    this.routerIp = routerIp;
    tid = (int) Util.to_number(flowset, templateOffset, 2);
    if (tid < 0 || tid > 255) {
      templateOffset += 2;
      int fieldCnt = (int) Util.to_number(flowset, templateOffset, 2);
      templateOffset += 2;

      int dataFlowSetOffset = 0;
      for (int idx = 0; idx < fieldCnt; idx++) {
        int typeName = (int) Util.to_number(flowset, templateOffset, 2);
        templateOffset += 2;
        int typeLen = (int) Util.to_number(flowset, templateOffset, 2);
        templateOffset += 2;
        if (typeName < MAX_FIELD_TYPE && typeName > 0) {
          fieldTypeMap.put(new Integer(typeName), new OffsetLengthObj(dataFlowSetOffset, typeLen));          
        }
        dataFlowSetOffset += typeLen;
      }
      totalLength = dataFlowSetOffset;
    } else {
      throw new Exception("Invalid TemplateId");
    }
  }
  
  

  public Map<Integer, OffsetLengthObj> getFieldTypeMap()
  {
    return fieldTypeMap;
  }

  /**
   * 
   * @param typeName
   * @return
   */
  public int getTypeOffset(int typeName)
  {
    if (fieldTypeMap.get(new Integer(typeName)) != null) {
      return fieldTypeMap.get(new Integer(typeName)).offset;
    }
    return -1;

  }

  public int getTypeLen(int typeName)
  {
    if (fieldTypeMap.get(new Integer(typeName)) != null) {
      return fieldTypeMap.get(new Integer(typeName)).length;
    }
    return 0;
  }
  
  public int getTid()
  {
    return tid;
  }

  public String getRouterIp()
  {
    return routerIp;
  }
  
  public int getTotalLength()
  {
    return totalLength;
  }

  public int getMAX_FIELD_TYPE()
  {
    return MAX_FIELD_TYPE;
  }

  public void setMAX_FIELD_TYPE(int mAX_FIELD_TYPE)
  {
    MAX_FIELD_TYPE = mAX_FIELD_TYPE;
  }
  
  public static class OffsetLengthObj{
    public int offset;
    public int length;
    public OffsetLengthObj(int offset, int length){
      this.offset = offset;
      this.length = length;
    }
  }

}
