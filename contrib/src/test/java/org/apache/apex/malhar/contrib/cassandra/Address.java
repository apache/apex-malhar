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
package org.apache.apex.malhar.contrib.cassandra;

import java.util.Set;

/**
 *
 */
public class Address
{
  private String city;

  private String street;

  private int zip_code;
  private Set<String> phones;

  public Address(String city, String street, int zipcode, Set<String> phones)
  {
    this.city = city;
    this.street = street;
    this.zip_code = zipcode;
    this.phones = phones;
  }

  public String getCity()
  {
    return city;
  }

  public void setCity(String city)
  {
    this.city = city;
  }

  public String getStreet()
  {
    return street;
  }

  public void setStreet(String street)
  {
    this.street = street;
  }

  public int getZip_code()
  {
    return zip_code;
  }

  public void setZip_code(int zip_code)
  {
    this.zip_code = zip_code;
  }

  public Set<String> getPhones()
  {
    return phones;
  }

  public void setPhones(Set<String> phones)
  {
    this.phones = phones;
  }
}
