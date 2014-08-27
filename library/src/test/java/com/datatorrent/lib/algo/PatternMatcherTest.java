package com.datatorrent.lib.algo;

import java.util.List;

import junit.framework.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 *
 */
public class PatternMatcherTest
{

  @Test
  public void test() throws Exception
  {
    CollectorTestSink sink = new CollectorTestSink();
    List<Integer> inputPattern = Lists.newArrayList();
    inputPattern.add(0);
    inputPattern.add(1);
    inputPattern.add(0);
    inputPattern.add(1);
    inputPattern.add(2);
    PatternMatcher.Pattern<Integer> pattern = new PatternMatcher.Pattern<Integer>(inputPattern);
    PatternMatcher<Integer> patternMatcher = new PatternMatcher<Integer>(pattern);
    patternMatcher.outputPort.setSink(sink);
    patternMatcher.setup(null);
    patternMatcher.beginWindow(0);
    patternMatcher.inputPort.process(0);
    patternMatcher.inputPort.process(1);
    patternMatcher.inputPort.process(1);
    patternMatcher.inputPort.process(0);
    patternMatcher.inputPort.process(1);
    patternMatcher.inputPort.process(0);
    patternMatcher.inputPort.process(1);
    patternMatcher.inputPort.process(2);
    patternMatcher.inputPort.process(1);
    patternMatcher.endWindow();
    patternMatcher.teardown();
  }

  @Test
  public void testPattern() throws Exception
  {
    CollectorTestSink sink = new CollectorTestSink();
    List<Integer> inputPattern = Lists.newArrayList();
    inputPattern.add(0);
    inputPattern.add(0);
    PatternMatcher.Pattern<Integer> pattern = new PatternMatcher.Pattern<Integer>(inputPattern);
    PatternMatcher<Integer> patternMatcher = new PatternMatcher<Integer>(pattern);
    patternMatcher.outputPort.setSink(sink);
    patternMatcher.setup(null);
    patternMatcher.beginWindow(0);
    patternMatcher.inputPort.process(0);
    patternMatcher.inputPort.process(0);
    patternMatcher.inputPort.process(0);
    patternMatcher.endWindow();
    patternMatcher.teardown();
    Assert.assertEquals("The number of tuples emited are two",2,sink.collectedTuples.size());
  }
}
