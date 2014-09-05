/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.distributeddistinct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.lib.algo.UniqueValueCount.InternalCountOutput;
import com.datatorrent.lib.bucket.Bucket;
import com.datatorrent.lib.bucket.BucketManagerImpl;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.helper.OperatorContextTestHelper.TestIdOperatorContext;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Test for {@link IntegerUniqueValueCountAppender} and {@link UniqueValueCountAppender}
 */
public class HDFSUnitTest
{
  private static final Logger logger = LoggerFactory.getLogger(DistributedDistinctTest.class);

  private final static String APP_ID = "HDFSUnitTest";
  private final static int OPERATOR_ID = 0;

  private static HDFSUniqueValueCountAppender<Integer> valueCounter;
  private static String applicationPath = "a";

  private static BucketManagerImpl<BucketableInternalCountOutput<Integer>> bucketManager;

  public final transient DefaultInputPort<KeyValPair<Object, Object>> input = new DefaultInputPort<KeyValPair<Object, Object>>() {
    public Map<Integer, Integer> map = new HashMap<Integer, Integer>();

    @Override
    public void process(KeyValPair<Object, Object> tuple)
    {
      map.put((Integer) tuple.getKey(), (Integer) tuple.getValue());
    }
  };


  @Test
  public void testProcess() throws Exception
  {
    // test insert
    insertValues();

    valueCounter.endWindow();
    long bucketKey = bucketManager.getBucketKeyFor(new BucketableInternalCountOutput<Integer>(1, 0, new HashSet<Object>()));
    Bucket<BucketableInternalCountOutput<Integer>> bucket = bucketManager.getBucket(bucketKey);
    BucketableInternalCountOutput<Integer> event = bucket.getValueFromWrittenPart(1);
    List<Integer> answersOne = new ArrayList<Integer>();
    for (int i = 1; i < 16; i++) {
      answersOne.add(i);
    }
    Assert.assertEquals(answersOne, processSet(event.getInternalSet()));

    bucketKey = bucketManager.getBucketKeyFor(new BucketableInternalCountOutput<Integer>(2, 0, new HashSet<Object>()));
    bucket = bucketManager.getBucket(bucketKey);
    event = bucket.getValueFromWrittenPart(2);
    List<Integer> answersTwo = new ArrayList<Integer>();
    answersTwo.add(3);
    answersTwo.add(6);
    answersTwo.add(9);
    for (int i = 11; i < 21; i++) {
      answersTwo.add(i);
    }
    Assert.assertEquals(answersTwo, processSet(event.getInternalSet()));

    bucketKey = bucketManager.getBucketKeyFor(new BucketableInternalCountOutput<Integer>(3, 0, new HashSet<Object>()));
    bucket = bucketManager.getBucket(bucketKey);
    event = bucket.getValueFromWrittenPart(3);

    List<Integer> answersThree = new ArrayList<Integer>();
    answersThree.add(2);
    answersThree.add(4);
    answersThree.add(6);
    answersThree.add(8);
    answersThree.add(10);
    for (int i = 11; i < 21; i++) {
      answersThree.add(i);
    }
    Assert.assertEquals(answersThree, processSet(event.getInternalSet()));
  }

  public static void insertValues()
  {
    logger.debug("start round 0");
    valueCounter.beginWindow(0);
    emitKeyVals(1, 1, 10, 1);
    emitKeyVals(1, 5, 15, 1);
    valueCounter.endWindow();
    logger.debug("end round 0");

    logger.debug("start round 1");
    valueCounter.beginWindow(1);
    emitKeyVals(2, 3, 15, 3);
    emitKeyVals(3, 2, 20, 2);
    emitKeyVals(3, 11, 20, 1);
    valueCounter.endWindow();
    logger.debug("end round 1");

    logger.debug("start round 2");
    valueCounter.beginWindow(2);
    emitKeyVals(3, 2, 20, 2);
    emitKeyVals(2, 11, 20, 1);
    valueCounter.endWindow();
    logger.debug("end round 2");
  }

  public static ArrayList<Integer> processSet(Set<Object> set)
  {
    ArrayList<Integer> tempList = new ArrayList<Integer>();
    for (Object o : set) {
      tempList.add((Integer) o);
    }
    Collections.sort(tempList);
    return tempList;
  }

  public static void emitKeyVals(int key, int start, int end, int increment)
  {
    int count = 0;
    Set<Object> valSet = new HashSet<Object>();
    for (int i = start; i <= end; i += increment) {
      count++;
      valSet.add(i);
    }
    valueCounter.processTuple(new InternalCountOutput<Integer>(key, count, valSet));
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    valueCounter = new HDFSUniqueValueCountAppender<Integer>();
    AttributeMap.DefaultAttributeMap attributes = new AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, APP_ID);
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    attributes.put(OperatorContext.ACTIVATION_WINDOW_ID, new Long(0));
    TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributes);
    bucketManager = new BucketManagerImpl<BucketableInternalCountOutput<Integer>>();
    HdfsBucketStore<BucketableInternalCountOutput<Integer>> store = new HdfsBucketStore<BucketableInternalCountOutput<Integer>>();
    HashSet<Integer> set = new HashSet<Integer>();
    set.add(0);
    store.setConfiguration(context.getId(), context.getValue(DAG.APPLICATION_PATH), set, 0);
    bucketManager.setNoOfBuckets(800);
    bucketManager.setWriteEventKeysOnly(false);
    bucketManager.setBucketStore(store);
    valueCounter.setBucketManager(bucketManager);
    valueCounter.setup(context);
  }
}
