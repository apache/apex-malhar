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
package org.apache.apex.malhar.lib.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Sink;

/**
 * Test for XmlKeyValueStringCartesianProduct
 */
public class XmlKeyValueStringCartesianProductTest
{

  @Test
  public void testProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><g>vg1</g></a>";
    String config = "a/b/c/d,a/b/c/e:a/b/f,a/g";
    List<String> collectedTuples = testOperator(xml, config);
    Assert.assertEquals("Output size", 4, collectedTuples.size());
    Assert.assertEquals("Output 1", "d=vd1,f=vf1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "d=vd1,g=vg1", collectedTuples.get(1));
    Assert.assertEquals("Output 3", "e=ve1,f=vf1", collectedTuples.get(2));
    Assert.assertEquals("Output 4", "e=ve1,g=vg1", collectedTuples.get(3));
  }

  @Test
  public void testGroupProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><g>vg1</g></a>";
    String config = "(a/b/c/d,a/b/c/e):a/b/f,a/g";
    List<String> collectedTuples = testOperator(xml, config);
    Assert.assertEquals("Output size", 2, collectedTuples.size());
    Assert.assertEquals("Output 1", "d=vd1,e=ve1,f=vf1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "d=vd1,e=ve1,g=vg1", collectedTuples.get(1));
  }

  @Test
  public void testSectionProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><b><c><d>vd2</d><e>ve2</e></c><f>vf2</f></b><g>vg1</g></a>";
    String config = "a/b#(c:f)";
    List<String> collectedTuples = testOperator(xml, config);
    Assert.assertEquals("Output size", 4, collectedTuples.size());
    Assert.assertEquals("Output 1", "d=vd1,f=vf1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "e=ve1,f=vf1", collectedTuples.get(1));
    Assert.assertEquals("Output 3", "d=vd2,f=vf2", collectedTuples.get(2));
    Assert.assertEquals("Output 4", "e=ve2,f=vf2", collectedTuples.get(3));
  }

  @Test
  public void testParentProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><g>vg1</g></a>";
    String config = "a/b/c:a/b/f,a/g";
    List<String> collectedTuples = testOperator(xml, config);
    Assert.assertEquals("Output size", 4, collectedTuples.size());
    Assert.assertEquals("Output 1", "d=vd1,f=vf1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "d=vd1,g=vg1", collectedTuples.get(1));
    Assert.assertEquals("Output 3", "e=ve1,f=vf1", collectedTuples.get(2));
    Assert.assertEquals("Output 4", "e=ve1,g=vg1", collectedTuples.get(3));
  }

  @Test
  public void testManyUnifiedParentProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><g>vg1</g><h><i>vi1</i><j>vj1</j></h></a>";
    String config = "a/g:(a/b/c),(a/h),a/b/f";
    List<String> collectedTuples = testOperator(xml, config);
    Assert.assertEquals("Output size", 3, collectedTuples.size());
    Assert.assertEquals("Output 1", "g=vg1,d=vd1,e=ve1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "g=vg1,i=vi1,j=vj1", collectedTuples.get(1));
    Assert.assertEquals("Output 3", "g=vg1,f=vf1", collectedTuples.get(2));
  }

  @Test
  public void testMultiProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><g>vg1</g><k>vk1</k></a>";
    String config = "a/b/c/d,a/b/c/e:a/b/f,a/g|a/b/c/e,a/g:a/b/f,a/k";
    List<String> collectedTuples = testOperator(xml, config);
    Assert.assertEquals("Output size", 8, collectedTuples.size());
    Assert.assertEquals("Output 1", "d=vd1,f=vf1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "d=vd1,g=vg1", collectedTuples.get(1));
    Assert.assertEquals("Output 3", "e=ve1,f=vf1", collectedTuples.get(2));
    Assert.assertEquals("Output 4", "e=ve1,g=vg1", collectedTuples.get(3));
    Assert.assertEquals("Output 5", "e=ve1,f=vf1", collectedTuples.get(4));
    Assert.assertEquals("Output 2", "e=ve1,k=vk1", collectedTuples.get(5));
    Assert.assertEquals("Output 3", "g=vg1,f=vf1", collectedTuples.get(6));
    Assert.assertEquals("Output 4", "g=vg1,k=vk1", collectedTuples.get(7));
  }

  List<String> testOperator(String xml, String config)
  {
    XmlKeyValueStringCartesianProduct operator = new XmlKeyValueStringCartesianProduct();
    operator.setConfig(config);
    operator.setup(null);
    final List<String> collectedTuples = new ArrayList<String>();
    operator.output.setSink(new Sink<Object>()
    {
      @Override
      public void put(Object o)
      {
        if (o instanceof String) {
          collectedTuples.add((String)o);
        }
      }

      @Override
      public int getCount(boolean b)
      {
        return collectedTuples.size();
      }
    });
    operator.input.process(xml);
    operator.teardown();
    return collectedTuples;
  }
}
