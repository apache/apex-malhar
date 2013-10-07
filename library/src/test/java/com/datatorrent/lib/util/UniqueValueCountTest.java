package com.datatorrent.lib.util;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.ImmutableMap;
import com.sun.research.ws.wadl.ResourceType;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Test for {@link UniqueValueCount} operator
 *
 * @since 0.3.5
 */
public class UniqueValueCountTest {
    private static Logger LOG = LoggerFactory.getLogger(UniqueValueCountTest.class);


    @Test
    public void uniqueCountTest(){
        UniqueValueCount<String,Integer> uniqueCountOper= new UniqueValueCount<String, Integer>();
        CollectorTestSink outputSink = new CollectorTestSink();
        uniqueCountOper.outputPort.setSink(outputSink);

        uniqueCountOper.beginWindow(0);
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test1",1));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test1",2));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test1",2));
        uniqueCountOper.endWindow();

        Assert.assertEquals("number emitted tuples", 1, outputSink.collectedTuples.size());
        KeyValPair<String,Integer> emittedPair= (KeyValPair <String,Integer>)outputSink.collectedTuples.get(0);
        Assert.assertEquals("emitted key was ", "test1", emittedPair.getKey());
        Assert.assertEquals("emitted value was ",2, emittedPair.getValue().intValue());

        outputSink.clear();
        uniqueCountOper.beginWindow(1);
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test1",1));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test1",2));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test1",2));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test2",1));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test2",2));
        uniqueCountOper.inputPort.process(new KeyValPair<String, Integer>("test2",2));
        uniqueCountOper.endWindow();

        ImmutableMap<String,Integer> answers=ImmutableMap.of("test1",2,"test2",2);

        Assert.assertEquals("number emitted tuples", 2, outputSink.collectedTuples.size());
        for(KeyValPair<String,Integer> emittedPair2: (List<KeyValPair<String,Integer>>)outputSink.collectedTuples) {
            Assert.assertEquals("emmit value of "+ emittedPair2.getKey() +" was ", answers.get(emittedPair2.getKey()), emittedPair2.getValue());
        }
        LOG.debug("Done unique count testing testing\n") ;
    }

}
