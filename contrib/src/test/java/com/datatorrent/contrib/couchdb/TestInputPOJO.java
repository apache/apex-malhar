/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.couchdb;

public class TestInputPOJO
{
  private InnerObj innerObj;
  // This field is optional.
  private String id;
  // This field is optional.
  private String revision;
  private String name = "test";
  private int age;

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public TestInputPOJO()
  {

  }

  public String getRevision()
  {
    return revision;
  }

  public void setRevision(String revision)
  {
    this.revision = revision;
  }

  public String getId()
  {
    return id;
  }

  public void setId(String id)
  {
    this.id = id;
  }

  /**
   * @return the innerObj
   */
  public InnerObj getInnerObj()
  {
    return innerObj;
  }

  /**
   * @param innerObj the innerObj to set
   */
  public void setInnerObj(InnerObj innerObj)
  {
    this.innerObj = innerObj;
  }

  protected static class InnerObj
  {
    public InnerObj()
    {
    }

    public int intVal = 10;
    public char charVal = 'c';
    public String stringVal = "hello";

    public char getCharVal()
    {
      return charVal;
    }

    public void setCharVal(char charVal)
    {
      this.charVal = charVal;
    }

    public String getStringVal()
    {
      return stringVal;
    }

    public void setStringVal(String stringVal)
    {
      this.stringVal = stringVal;
    }

    public int getIntVal()
    {
      return intVal;
    }

    public void setIntVal(int intVal)
    {
      this.intVal = intVal;
    }

  }

}
