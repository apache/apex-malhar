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
package org.apache.apex.malhar.sql;


import java.util.Date;

public class OutputPOJO
{
  private Date RowTime1;
  private Date RowTime2;
  private String Product;

  public Date getRowTime1()
  {
    return RowTime1;
  }

  public void setRowTime1(Date rowTime1)
  {
    RowTime1 = rowTime1;
  }

  public Date getRowTime2()
  {
    return RowTime2;
  }

  public void setRowTime2(Date rowTime2)
  {
    RowTime2 = rowTime2;
  }

  public String getProduct()
  {
    return Product;
  }

  public void setProduct(String product)
  {
    Product = product;
  }
}
