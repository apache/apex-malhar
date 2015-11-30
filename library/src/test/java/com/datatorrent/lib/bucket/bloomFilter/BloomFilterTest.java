/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.bucket.bloomFilter;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Test for {@link BloomFilter}
 * <p>
 *
 */
public class BloomFilterTest
{
  @Test
  public void testBloomFilter()
  {
    BloomFilter<Integer> bf = new BloomFilter<Integer>(1000000, 0.01);

    for (int i = 0; i < 1000000; i++) {
      if (i % 2 == 0) {
        bf.add(i);
      }
    }

    int falsePositive = 0;
    for (int i = 0; i < 1000000; i++) {
      if (!bf.contains(i)) {
        Assert.assertTrue(i % 2 != 0);
      } else {
        // BF says its present
        if (i % 2 != 0) {
          // But was not there
          falsePositive++;
        }
      }
    }
    // Verify false positive prob
    double falsePositiveProb = falsePositive / 1000000.0;
    Assert.assertTrue(falsePositiveProb <= 0.01);
  }
}
