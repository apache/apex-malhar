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
package org.apache.apex.malhar.contrib.enrich;

public class EmployeeOrder
{
  public int OID;
  public int ID;
  public double amount;
  public String NAME;
  public int AGE;
  public String ADDRESS;
  public double SALARY;

  public int getOID()
  {
    return OID;
  }

  public void setOID(int OID)
  {
    this.OID = OID;
  }

  public int getID()
  {
    return ID;
  }

  public void setID(int ID)
  {
    this.ID = ID;
  }

  public int getAGE()
  {
    return AGE;
  }

  public void setAGE(int AGE)
  {
    this.AGE = AGE;
  }

  public String getNAME()
  {
    return NAME;
  }

  public void setNAME(String NAME)
  {
    this.NAME = NAME;
  }

  public double getAmount()
  {
    return amount;
  }

  public void setAmount(double amount)
  {
    this.amount = amount;
  }

  public String getADDRESS()
  {
    return ADDRESS;
  }

  public void setADDRESS(String ADDRESS)
  {
    this.ADDRESS = ADDRESS;
  }

  public double getSALARY()
  {
    return SALARY;
  }

  public void setSALARY(double SALARY)
  {
    this.SALARY = SALARY;
  }

  @Override
  public String toString()
  {
    return "{" +
        "OID=" + OID +
        ", ID=" + ID +
        ", amount=" + amount +
        ", NAME='" + NAME + '\'' +
        ", AGE=" + AGE +
        ", ADDRESS='" + ADDRESS.trim() + '\'' +
        ", SALARY=" + SALARY +
        '}';
  }
}
