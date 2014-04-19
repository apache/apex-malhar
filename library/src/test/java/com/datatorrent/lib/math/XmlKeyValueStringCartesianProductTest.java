package com.datatorrent.lib.math;

import com.datatorrent.api.Sink;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 4/19/14.
 */
public class XmlKeyValueStringCartesianProductTest
{
  @Test
  public void testProduct() throws IOException
  {
    String xml = "<a><b><c><d>vd1</d><e>ve1</e></c><f>vf1</f></b><b><c><d>vd2</d><e>ve2</e></c><f>vf2</f></b></a>";
    //String config = "a/b/c/d,a/b/c/e:a/b/f";
    //String config = "a/b/c:a/b/f";
    String config = "a/b#(c:f)";
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
          collectedTuples.add((String) o);
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
    Assert.assertEquals("Output size", 4, collectedTuples.size());
    Assert.assertEquals("Output 1", "d=vd1,f=vf1", collectedTuples.get(0));
    Assert.assertEquals("Output 2", "e=ve1,f=vf1", collectedTuples.get(1));
    Assert.assertEquals("Output 3", "d=vd2,f=vf2", collectedTuples.get(2));
    Assert.assertEquals("Output 4", "e=ve2,f=vf2", collectedTuples.get(3));
  }
}
