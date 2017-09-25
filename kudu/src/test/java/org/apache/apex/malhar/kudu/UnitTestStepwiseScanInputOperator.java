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
package org.apache.apex.malhar.kudu;


public class UnitTestStepwiseScanInputOperator extends AbstractKuduInputOperator<UnitTestTablePojo,InputOperatorControlTuple>
{
  private int start = 0;

  private int step = 2;

  public UnitTestStepwiseScanInputOperator(ApexKuduConnection.ApexKuduConnectionBuilder kuduConnectionInfo,
      Class<UnitTestTablePojo> clazzForPOJO) throws Exception
  {
    super(kuduConnectionInfo, clazzForPOJO);
  }

  public UnitTestStepwiseScanInputOperator()
  {
  }

  @Override
  protected String getNextQuery()
  {
    return "select timestamprowkey,longdata,stringdata from unittests where introwkey >= " + start +
        "and introwkey <= " + (start++ + step);
  }
}
