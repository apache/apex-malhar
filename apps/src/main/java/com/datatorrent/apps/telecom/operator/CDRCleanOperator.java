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
package com.datatorrent.apps.telecom.operator;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.FilterOperator;

public class CDRCleanOperator extends FilterOperator<Map<String, Object>>
{
  private transient List<CDRValidator> vList = new LinkedList<CDRCleanOperator.CDRValidator>();
  
  @OutputPortFieldAnnotation(name="invalid cdr output", optional=true)
  public transient DefaultOutputPort<Map<String, Object>> invalidOutput = new DefaultOutputPort<Map<String,Object>>();

  @Override
  public boolean satisfiesFilter(Map<String, Object> tuple)
  {
    for (CDRValidator v : vList) {
      if(!v.validateTuple(tuple)){
        return false;
      }
    }
    return true;
  }

  @Override
  public void handleInvalidTuple(Map<String, Object> tuple)
  {
    invalidOutput.emit(tuple);
  }

  
  void registValidator(CDRValidator validator){
    vList.add(validator);
  }
  
  
  
  public static interface CDRValidator{
    boolean validateTuple(Map<String, Object> tuple);
  }
  
}
