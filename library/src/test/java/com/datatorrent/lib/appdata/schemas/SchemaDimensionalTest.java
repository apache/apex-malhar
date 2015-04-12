/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import org.junit.Test;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaDimensionalTest
{
  public SchemaDimensionalTest()
  {
  }

  @Test
  public void simpleTest()
  {
    /*
    String json = "{\"schemaType\":\"dimensions\","
            + "\"schemaVersion\":\"1.0\","
            + "\"time\":"
            + "{\"from\":\"2015-03-19 00:00:00:000\","
            + "\"to\":\"2015-03-21 17:02:00:000\","
            + "\"buckets\":[\"1m\",\"1h\",\"1d\"]},"
            + "\"keys\":[{\"name\":\"publisher\",\"type\":\"string\",\"enumValues\":[\"twitter\",\"facebook\"]},"
            + "          {\"name\":\"advertiser\",\"type\":\"string\",\"enumValues\":[\"starbucks\",\"safeway\"]},"
            + "          {\"name\":\"location\",\"type\":\"string\",\"enumValues\":[\"N\",\"LREC\",\"SKY\"]}],"
            + "\"values\":[{\"name\":\"impressions\",\"type\":\"integer\"},"
            + "            {\"name\":\"clicks\",\"type\":\"integer\"},"
            + "            {\"name\":\"cost\",\"type\":\"float\"},"
            + "            {\"name\":\"revenue\",\"type\":\"float\"}]}";

    SchemaDimensional gsd = new SchemaDimensional(json);

    Assert.assertEquals("Schema versions must match.", "1.0", gsd.getSchemaVersion());
    Assert.assertEquals("Schema types must match.", "dimensions", gsd.getSchemaType());

    Assert.assertEquals("From must match.", "2015-03-19 00:00:00:000", gsd.getFrom());
    Assert.assertEquals("To must match.", "2015-03-21 17:02:00:000", gsd.getTo());

    Set<TimeBucket> timeBuckets = Sets.newHashSet(TimeBucket.MINUTE, TimeBucket.HOUR, TimeBucket.DAY);
    Assert.assertEquals("Time buckets must match.", timeBuckets, gsd.getBuckets());

    Map<String, Type> keyToType = Maps.newHashMap();
    keyToType.put("publisher", Type.STRING);
    keyToType.put("advertiser", Type.STRING);
    keyToType.put("location", Type.STRING);

    Assert.assertEquals("Keys and types must match.", keyToType, gsd.getKeyToType());

    Map<String, Set<Object>> keyToValues = Maps.newHashMap();
    keyToValues.put("publisher", Sets.newHashSet((Object) "twitter", (Object) "facebook"));
    keyToValues.put("advertiser", Sets.newHashSet((Object) "starbucks", (Object) "safeway"));
    keyToValues.put("location", Sets.newHashSet((Object) "N", (Object) "LREC", (Object) "SKY"));

    Assert.assertEquals("Keys and values must match.", keyToValues, gsd.getKeyToValues());

    Map<String, Type> valuesToType = Maps.newHashMap();
    valuesToType.put("impressions", Type.INTEGER);
    valuesToType.put("clicks", Type.INTEGER);
    valuesToType.put("cost", Type.FLOAT);
    valuesToType.put("revenue", Type.FLOAT);

    Assert.assertEquals("Values to types must match.", valuesToType, gsd.getFieldToType());*/
  }
}
