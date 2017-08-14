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
package org.apache.apex.malhar.lib.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator.DirectoryScanner;
import org.apache.apex.malhar.lib.partitioner.StatelessPartitionerTest.PartitioningContextImpl;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner.Partition;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StatsListener;

public class AbstractFileInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH, dir);
      context = mockOperatorContext(1, attributes);
    }

    @Override
    protected void finished(Description description)
    {
      TestUtils.deleteTargetTestClassFolder(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSinglePartitonRecursive() throws Exception
  {
    checkSubDir(true);
  }

  @Test
  public void testSinglePartiton() throws Exception
  {
    checkSubDir(false);
  }

  private void checkSubDir(boolean recursive) throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    HashSet<String> allLines = Sets.newHashSet();
    String subdir = "";
    for (int file = 0; file < 2; file++) {
      subdir += String.format("/depth_%d", file);
      HashSet<String> lines = Sets.newHashSet();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir + subdir, "file" + file), StringUtils.join(lines, '\n'));
    }

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp("((?!target).)*file[\\d]");
    oper.getScanner().setRecursive(recursive);

    oper.setup(testMeta.context);
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    int expectedNumTuples = 4;
    if (!recursive) {
      allLines = new HashSet<String>();
      expectedNumTuples = 0;
    }
    Assert.assertEquals("number tuples", expectedNumTuples, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, new HashSet<String>(queryResults.collectedTuples));

  }

  public static class LineOperator extends LineByLineFileInputOperator
  {
    Set<String> dirPaths = Sets.newHashSet();

    @Override
    protected void visitDirectory(Path filePath)
    {
      dirPaths.add(Path.getPathWithoutSchemeAndAuthority(filePath).toString());
    }
  }

  @Test
  public void testEmptyDirectory() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    Set<String> dPaths = Sets.newHashSet();
    dPaths.add(new File(testMeta.dir).getCanonicalPath());

    String subdir01 = "/a";
    dPaths.add(new File(testMeta.dir + subdir01).getCanonicalPath());
    FileUtils.forceMkdir((new File(testMeta.dir + subdir01)));

    String subdir02 = "/b";
    dPaths.add(new File(testMeta.dir + subdir02).getCanonicalPath());
    FileUtils.forceMkdir(new File(testMeta.dir + subdir02));

    String subdir03 = subdir02 + "/c";
    dPaths.add(new File(testMeta.dir + subdir03).getCanonicalPath());
    FileUtils.forceMkdir(new File(testMeta.dir + subdir03));

    String subdir04 = "/d";
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }
    allLines.addAll(lines);
    File testFile = new File(testMeta.dir + subdir04, "file0");
    dPaths.add(new File(testMeta.dir + subdir04).getCanonicalPath());
    FileUtils.write(testFile, StringUtils.join(lines, '\n'));

    LineOperator oper = new LineOperator();
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    // Read all records to populate processedList in operator.
    oper.setup(testMeta.context);
    for (int i = 0; i < 3; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }

    Assert.assertEquals("Size", 5, oper.dirPaths.size());
    Assert.assertTrue("Checking Sets", dPaths.equals(oper.dirPaths));
  }

  @Test
  public void testScannerPartitioning() throws Exception
  {
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setFilePatternRegexp(".*partition([\\d]*)");

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file = 0; file < 4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "");
    }

    FileSystem fs = FileSystem.get(FileContext.getLocalFSFileContext().getDefaultFileSystem().getUri(), new Configuration());
    List<DirectoryScanner> partitions = scanner.partition(2);
    Set<Path> allFiles = Sets.newHashSet();
    for (DirectoryScanner partition : partitions) {
      Set<Path> files = partition.scan(fs, path, Sets.<String>newHashSet());
      Assert.assertEquals("", 3, files.size());
      allFiles.addAll(files);
    }
    Assert.assertEquals("Found all files " + allFiles, 5, allFiles.size());

  }

  @Test
  public void testPartitioning() throws Exception
  {
    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file = 0; file < 4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "");
    }

    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));
    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = oper.definePartitions(partitions,
        new PartitioningContextImpl(null, 2));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, oper.getCurrentPartitions()); // partitioned() wasn't called

    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      Assert.assertNotSame(oper, p.getPartitionedInstance());
      Assert.assertNotSame(oper.getScanner(), p.getPartitionedInstance().getScanner());
      Set<String> consumed = Sets.newHashSet();
      LinkedHashSet<Path> files = p.getPartitionedInstance().getScanner().scan(FileSystem.getLocal(new Configuration(false)), path, consumed);
      Assert.assertEquals("partition " + files, 3, files.size());
    }
  }

  /**
   * Test for testing dynamic partitioning.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read all records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit records again, expected result is no record is emitted, as all files are
   *   processed.
   * - Create another 4 files with 3 records each
   * - Try to emit records again, expected result total record emitted 4 * 3 = 12.
   */
  @Test
  public void testPartitioningStateTransfer() throws Exception
  {

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);

    LineByLineFileInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file;
    for (file = 0; file < 4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    // Read all records to populate processedList in operator.
    oper.setup(testMeta.context);
    for (int i = 0; i < 10; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }
    Assert.assertEquals("All tuples read ", 12, sink.collectedTuples.size());

    Assert.assertEquals(1, initialState.getCurrentPartitions());
    initialState.setPartitionCount(2);
    StatsListener.Response rsp = initialState.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    // Create partitions of the operator.
    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));
    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = initialState.definePartitions(partitions,
        new PartitioningContextImpl(null, 0));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, initialState.getCurrentPartitions());
    Map<Integer, Partition<AbstractFileInputOperator<String>>> m = Maps.newHashMap();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      m.put(m.size(), p);
    }
    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    /* Collect all operators in a list */
    List<AbstractFileInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      LineByLineFileInputOperator oi = (LineByLineFileInputOperator)p.getPartitionedInstance();
      oi.setup(testMeta.context);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for (int i = 0; i < 10; i++) {
      for (AbstractFileInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // No record should be read.
    Assert.assertEquals("No new tuples read ", 0, sink.collectedTuples.size());

    // Add four new files with 3 records each.
    for (; file < 8; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "a\nb\nc\n");
    }

    for (int i = 0; i < 10; i++) {
      for (AbstractFileInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // If all files are processed only once then number of records emitted should
    // be 12.
    Assert.assertEquals("All tuples read ", 12, sink.collectedTuples.size());
  }

  /**
   * Test for testing dynamic partitioning.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read some records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit the remaining records.
   */
  @Test
  public void testPartitioningStateTransferInterrupted() throws Exception
  {
    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);
    oper.setEmitBatchSize(2);

    LineByLineFileInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file;
    for (file = 0; file < 4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    //Read some records
    oper.setup(testMeta.context);
    for (int i = 0; i < 5; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }

    Assert.assertEquals("Partial tuples read ", 6, sink.collectedTuples.size());

    Assert.assertEquals(1, initialState.getCurrentPartitions());
    initialState.setPartitionCount(2);
    StatsListener.Response rsp = initialState.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    // Create partitions of the operator.
    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));
    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = initialState.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, initialState.getCurrentPartitions());
    Map<Integer, Partition<AbstractFileInputOperator<String>>> m = Maps.newHashMap();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      m.put(m.size(), p);
    }
    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    /* Collect all operators in a list */
    List<AbstractFileInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      LineByLineFileInputOperator oi = (LineByLineFileInputOperator)p.getPartitionedInstance();
      oi.setup(testMeta.context);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for (int i = 0; i < 10; i++) {
      for (AbstractFileInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    Assert.assertEquals("Remaining tuples read ", 6, sink.collectedTuples.size());
  }

  /**
   * Test for testing dynamic partitioning interrupting ongoing read.
   * - Create 4 file with 3 records each.
   * - Create a single partition, and read some records, populating pending files in operator.
   * - Split it in two operators
   * - Try to emit the remaining records.
   */
  @Test
  public void testPartitioningStateTransferFailure() throws Exception
  {
    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);
    oper.setEmitBatchSize(2);

    LineByLineFileInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file;
    for (file = 0; file < 4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    //Read some records
    oper.setup(testMeta.context);
    for (int i = 0; i < 5; i++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      wid++;
    }

    Assert.assertEquals("Partial tuples read ", 6, sink.collectedTuples.size());

    Assert.assertEquals(1, initialState.getCurrentPartitions());
    initialState.setPartitionCount(2);
    StatsListener.Response rsp = initialState.processStats(null);
    Assert.assertEquals(true, rsp.repartitionRequired);

    // Create partitions of the operator.
    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));
    // incremental capacity controlled partitionCount property
    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = initialState.definePartitions(partitions, new PartitioningContextImpl(null, 0));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, initialState.getCurrentPartitions());
    Map<Integer, Partition<AbstractFileInputOperator<String>>> m = Maps.newHashMap();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      m.put(m.size(), p);
    }
    initialState.partitioned(m);
    Assert.assertEquals(2, initialState.getCurrentPartitions());

    /* Collect all operators in a list */
    List<AbstractFileInputOperator<String>> opers = Lists.newArrayList();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      LineByLineFileInputOperator oi = (LineByLineFileInputOperator)p.getPartitionedInstance();
      oi.setup(testMeta.context);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for (int i = 0; i < 10; i++) {
      for (AbstractFileInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // No record should be read.
    Assert.assertEquals("Remaining tuples read ", 6, sink.collectedTuples.size());
  }

  @Test
  public void testRecoveryWithFailedFile() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }
    allLines.addAll(lines);
    File testFile = new File(testMeta.dir, "file0");
    FileUtils.write(testFile, StringUtils.join(lines, '\n'));


    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.scanner = null;
    oper.failedFiles.add(new AbstractFileInputOperator.FailedFile(testFile.getAbsolutePath(), 1));

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);

    oper.setup(testMeta.context);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    oper.teardown();

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines.subList(1, allLines.size()), new ArrayList<String>(queryResults.collectedTuples));
  }

  @Test
  public void testRecoveryWithUnfinishedFile() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }
    allLines.addAll(lines);
    File testFile = new File(testMeta.dir, "file0");
    FileUtils.write(testFile, StringUtils.join(lines, '\n'));

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.scanner = null;
    oper.unfinishedFiles.add(new AbstractFileInputOperator.FailedFile(testFile.getAbsolutePath(), 2));

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);

    oper.setup(testMeta.context);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    oper.teardown();

    Assert.assertEquals("number tuples", 3, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines.subList(2, allLines.size()), new ArrayList<String>(queryResults.collectedTuples));
  }

  @Test
  public void testRecoveryWithPendingFile() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }
    allLines.addAll(lines);
    File testFile = new File(testMeta.dir, "file0");
    FileUtils.write(testFile, StringUtils.join(lines, '\n'));

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.scanner = null;
    oper.pendingFiles.add(testFile.getAbsolutePath());

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);

    oper.setup(testMeta.context);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    oper.teardown();

    Assert.assertEquals("number tuples", 5, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, new ArrayList<String>(queryResults.collectedTuples));
  }

  @Test
  public void testRecoveryWithCurrentFile() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }
    allLines.addAll(lines);
    File testFile = new File(testMeta.dir, "file0");
    FileUtils.write(testFile, StringUtils.join(lines, '\n'));

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.scanner = null;
    oper.currentFile = testFile.getAbsolutePath();
    oper.offset = 1;

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);

    oper.setup(testMeta.context);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    oper.teardown();

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines.subList(1, allLines.size()), new ArrayList<String>(queryResults.collectedTuples));
  }

  @Test
  public void testIdempotency() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    List<String> allLines = Lists.newArrayList();
    for (int file = 0; file < 2; file++) {
      List<String> lines = Lists.newArrayList();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");

    oper.setWindowDataManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestUtils.setSink(oper.output, queryResults);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(testMeta.context);
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();
    List<String> beforeRecovery = Lists.newArrayList(queryResults.collectedTuples);

    queryResults.clear();

    //idempotency  part
    oper.setup(testMeta.context);
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.endWindow();
    }
    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", beforeRecovery, queryResults.collectedTuples);
    oper.teardown();
  }

  @Test
  public void testIdempotencyWithMultipleEmitTuples() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    List<String> allLines = Lists.newArrayList();
    for (int file = 0; file < 2; file++) {
      List<String> lines = Lists.newArrayList();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");

    oper.setWindowDataManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    TestUtils.setSink(oper.output, queryResults);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(testMeta.context);
    oper.beginWindow(0);
    for (int i = 0; i < 3; i++) {
      oper.emitTuples();
    }
    oper.endWindow();
    oper.teardown();
    List<String> beforeRecovery = Lists.newArrayList(queryResults.collectedTuples);

    queryResults.clear();

    //idempotency  part
    oper.setup(testMeta.context);
    oper.beginWindow(0);
    oper.endWindow();
    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", beforeRecovery, queryResults.collectedTuples);
    oper.teardown();
  }

  @Test
  public void testIdempotencyWhenFileContinued() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    List<String> lines = Lists.newArrayList();
    for (int line = 0; line < 10; line++) {
      lines.add("l" + line);
    }
    FileUtils.write(new File(testMeta.dir, "file0"), StringUtils.join(lines, '\n'));

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");
    oper.setEmitBatchSize(5);

    oper.setWindowDataManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(testMeta.context);
    int offset = 0;
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
      if (wid > 0) {
        Assert.assertEquals("number tuples", 5, queryResults.collectedTuples.size());
        Assert.assertEquals("lines", lines.subList(offset, offset + 5), queryResults.collectedTuples);
        offset += 5;
      }
      sink.clear();
    }
    oper.teardown();
    sink.clear();

    //idempotency  part
    offset = 0;
    oper.setup(testMeta.context);
    for (long wid = 0; wid < 3; wid++) {
      oper.beginWindow(wid);
      oper.endWindow();
      if (wid > 0) {
        Assert.assertEquals("number tuples", 5, queryResults.collectedTuples.size());
        Assert.assertEquals("lines", lines.subList(offset, offset + 5), queryResults.collectedTuples);
        offset += 5;
      }
      sink.clear();
    }
    oper.teardown();
  }

  @Test
  public void testStateWithIdempotency() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    HashSet<String> allLines = Sets.newHashSet();
    for (int file = 0; file < 3; file++) {
      HashSet<String> lines = Sets.newHashSet();
      for (int line = 0; line < 2; line++) {
        lines.add("f" + file + "l" + line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file" + file), StringUtils.join(lines, '\n'));
    }

    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();

    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(testMeta.dir + "/recovery");

    oper.setWindowDataManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink)queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(testMeta.context);
    for (long wid = 0; wid < 4; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    sink.clear();

    //idempotency  part
    oper.pendingFiles.add(new File(testMeta.dir, "file0").getAbsolutePath());
    oper.failedFiles.add(new AbstractFileInputOperator.FailedFile(new File(testMeta.dir, "file1").getAbsolutePath(), 0));
    oper.unfinishedFiles.add(new AbstractFileInputOperator.FailedFile(new File(testMeta.dir, "file2").getAbsolutePath(), 0));

    oper.setup(testMeta.context);
    for (long wid = 0; wid < 4; wid++) {
      oper.beginWindow(wid);
      oper.endWindow();
    }
    Assert.assertTrue("pending state", !oper.pendingFiles.contains("file0"));

    for (AbstractFileInputOperator.FailedFile failedFile : oper.failedFiles) {
      Assert.assertTrue("failed state", !failedFile.path.equals("file1"));
    }

    for (AbstractFileInputOperator.FailedFile unfinishedFile : oper.unfinishedFiles) {
      Assert.assertTrue("unfinished state", !unfinishedFile.path.equals("file2"));
    }
    oper.teardown();
  }

  @Test
  public void testIdempotencyWithCheckPoint() throws Exception
  {
    testIdempotencyWithCheckPoint(new LineByLineFileInputOperator(), new CollectorTestSink<String>(), new IdempotencyTestDriver<LineByLineFileInputOperator>()
    {
      @Override
      public void writeFile(int count, String fileName) throws IOException
      {
        List<String> lines = Lists.newArrayList();
        for (int line = 0; line < count; line++) {
          lines.add(fileName + "l" + line);
        }
        FileUtils.write(new File(testMeta.dir, fileName), StringUtils.join(lines, '\n'));
      }

      @Override
      public void setSink(LineByLineFileInputOperator operator, Sink<?> sink)
      {
        TestUtils.setSink(operator.output, sink);
      }

      @Override
      public String getDirectory()
      {
        return testMeta.dir;
      }

      @Override
      public Context.OperatorContext getContext()
      {
        return testMeta.context;
      }
    });
  }

  public interface IdempotencyTestDriver<T extends Operator>
  {
    void writeFile(int count, String fileName) throws IOException;

    void setSink(T operator, Sink<?> sink);

    String  getDirectory();

    Context.OperatorContext getContext();
  }

  public static <S extends AbstractFileInputOperator, T> void testIdempotencyWithCheckPoint(S oper, CollectorTestSink<T> queryResults, IdempotencyTestDriver<S> driver) throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(driver.getDirectory()).getAbsolutePath()), true);

    int file = 0;
    driver.writeFile(5, "file" + file);

    file = 1;
    driver.writeFile(6, "file" + file);

    // empty file
    file = 2;
    driver.writeFile(0, "file" + file);

    FSWindowDataManager manager = new FSWindowDataManager();
    manager.setStatePath(driver.getDirectory() + "/recovery");

    oper.setWindowDataManager(manager);

    oper.setDirectory(driver.getDirectory());
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(driver.getContext());

    oper.setEmitBatchSize(3);

    // sort the pendingFiles and ensure the ordering of the files scanned
    DirectoryScannerNew newScanner = new DirectoryScannerNew();
    oper.setScanner(newScanner);

    // scan directory
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();

    // emit f0l0, f0l1, f0l2
    oper.beginWindow(1);
    oper.emitTuples();
    oper.endWindow();

    //checkpoint the operator
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    S checkPointOper = checkpoint(oper, bos);

    // start saving output
    driver.setSink(oper, queryResults);

    // emit f0l3, f0l4, and closeFile(f0) in the same window
    oper.beginWindow(2);
    oper.emitTuples();
    oper.endWindow();
    List<T> beforeRecovery2 = Lists.newArrayList(queryResults.collectedTuples);

    // emit f1l0, f1l1, f1l2
    oper.beginWindow(3);
    oper.emitTuples();
    oper.endWindow();
    List<T> beforeRecovery3 = Lists.newArrayList(queryResults.collectedTuples);

    // emit f1l3, f1l4, f1l5
    oper.beginWindow(4);
    oper.emitTuples();
    oper.endWindow();
    List<T> beforeRecovery4 = Lists.newArrayList(queryResults.collectedTuples);

    // closeFile(f1) in a new window
    oper.beginWindow(5);
    oper.emitTuples();
    oper.endWindow();
    List<T> beforeRecovery5 = Lists.newArrayList(queryResults.collectedTuples);

    // empty file ops, closeFile(f2) in emitTuples() only
    oper.beginWindow(6);
    oper.emitTuples();
    oper.endWindow();
    List<T> beforeRecovery6 = Lists.newArrayList(queryResults.collectedTuples);

    oper.teardown();

    queryResults.clear();

    //idempotency  part

    oper = restoreCheckPoint(checkPointOper, bos);
    driver.getContext().getAttributes().put(Context.OperatorContext.ACTIVATION_WINDOW_ID, 1L);
    oper.setup(driver.getContext());
    driver.setSink(oper, queryResults);

    long startwid = driver.getContext().getAttributes().get(Context.OperatorContext.ACTIVATION_WINDOW_ID) + 1;

    oper.beginWindow(startwid);
    Assert.assertTrue(oper.currentFile == null);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery2, queryResults.collectedTuples);

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery3, queryResults.collectedTuples);

    oper.beginWindow(++startwid);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery4, queryResults.collectedTuples);

    oper.beginWindow(++startwid);
    Assert.assertTrue(oper.currentFile == null);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery5, queryResults.collectedTuples);

    oper.beginWindow(++startwid);
    Assert.assertTrue(oper.currentFile == null);
    oper.emitTuples();
    oper.endWindow();
    Assert.assertEquals("lines", beforeRecovery6, queryResults.collectedTuples);

    Assert.assertEquals("number tuples", 8, queryResults.collectedTuples.size());

    oper.teardown();
  }

  /**
   * This method checkpoints the given operator.
   * @param oper The operator to checkpoint.
   * @param bos The ByteArrayOutputStream which saves the checkpoint data temporarily.
   * @return new operator.
   */
  public static <T> T checkpoint(T oper, ByteArrayOutputStream bos) throws Exception
  {
    Kryo kryo = new Kryo();

    Output loutput = new Output(bos);
    kryo.writeObject(loutput, oper);
    loutput.close();

    Input lInput = new Input(bos.toByteArray());
    @SuppressWarnings("unchecked")
    T checkPointedOper = kryo.readObject(lInput, (Class<T>)oper.getClass());
    lInput.close();

    return checkPointedOper;
  }

  /**
   * Restores the checkpointed operator.
   * @param checkPointOper The checkpointed operator.
   * @param bos The ByteArrayOutputStream which saves the checkpoint data temporarily.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T> T restoreCheckPoint(T checkPointOper, ByteArrayOutputStream bos) throws Exception
  {
    Kryo kryo = new Kryo();

    Input lInput = new Input(bos.toByteArray());
    T oper = kryo.readObject(lInput, (Class<T>)checkPointOper.getClass());
    lInput.close();

    return oper;
  }

  @Test
  public void testWindowDataManagerPartitioning() throws Exception
  {
    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setWindowDataManager(new FSWindowDataManager());
    oper.operatorId = 7;

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file = 0; file < 4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00" + file), "");
    }

    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));

    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = oper.definePartitions(partitions, new PartitioningContextImpl(null, 2));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, oper.getCurrentPartitions());

    List<FSWindowDataManager> storageManagers = Lists.newLinkedList();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      storageManagers.add((FSWindowDataManager)p.getPartitionedInstance().getWindowDataManager());
    }
    Assert.assertEquals("count of storage managers", 2, storageManagers.size());

    int countOfDeleteManagers = 0;
    FSWindowDataManager deleteManager = null;
    for (FSWindowDataManager storageManager : storageManagers) {
      if (storageManager.getDeletedOperators() != null) {
        countOfDeleteManagers++;
        deleteManager = storageManager;
      }
    }

    Assert.assertEquals("count of delete managers", 1, countOfDeleteManagers);
    Assert.assertNotNull("deleted operators manager", deleteManager);
    Assert.assertEquals("deleted operators", Sets.newHashSet(7), deleteManager.getDeletedOperators());
  }

  /** scanner to extract partition id from start of the filename */
  static class MyScanner extends AbstractFileInputOperator.DirectoryScanner
  {
    @Override
    protected int getPartition(String filePathStr)
    {
      String[] parts = filePathStr.split("/");
      parts = parts[parts.length - 1].split("_");
      try {
        int code = Integer.parseInt(parts[0]);
        return code;
      } catch (NumberFormatException ex) {
        return super.getPartition(filePathStr);
      }
    }
  }

  /**
   * Partition the operator in 2
   * create ten files with index of the file at the start, i.e 1_file, 2_file .. etc.
   * The scanner returns this index from getPartition method.
   * each partition should read 5 files as file index are from 0 to 9 (including 0 and 9).
   * @throws Exception
   */
  @Test
  public void testWithCustomScanner() throws Exception
  {
    LineByLineFileInputOperator oper = new LineByLineFileInputOperator();
    oper.setScanner(new MyScanner());
    oper.getScanner().setFilePatternRegexp(".*partition_([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    Random rand = new Random();
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file = 0; file < 10; file++) {
      FileUtils.write(new File(testMeta.dir, file + "_partition_00" + rand.nextInt(100)), "");
    }

    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));
    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = oper.definePartitions(partitions,
        new PartitioningContextImpl(null, 2));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, oper.getCurrentPartitions()); // partitioned() wasn't called

    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      Assert.assertNotSame(oper, p.getPartitionedInstance());
      Assert.assertNotSame(oper.getScanner(), p.getPartitionedInstance().getScanner());
      Set<String> consumed = Sets.newHashSet();
      LinkedHashSet<Path> files = p.getPartitionedInstance().getScanner().scan(FileSystem.getLocal(new Configuration(false)), path, consumed);
      Assert.assertEquals("partition " + files, 6, files.size());
    }
  }

  @Test
  public void testCustomScanner()
  {
    MyScanner scanner = new MyScanner();
    scanner.setPartitionCount(2);

    scanner.setPartitionIndex(1);
    boolean accepted = scanner.acceptFile("1_file");
    Assert.assertTrue("File should be accepted by this partition ", accepted);

    scanner.setPartitionIndex(0);
    accepted = scanner.acceptFile("1_file");
    Assert.assertFalse("File should not be accepted by this partition ", accepted);
  }

  private static class DirectoryScannerNew extends DirectoryScanner
  {
    public LinkedHashSet<Path> scan(FileSystem fs, Path filePath, Set<String> consumedFiles)
    {
      LinkedHashSet<Path> pathSet;
      pathSet = super.scan(fs, filePath, consumedFiles);

      TreeSet<Path> orderFiles = new TreeSet<>();
      orderFiles.addAll(pathSet);
      pathSet.clear();
      Iterator<Path> fileIterator = orderFiles.iterator();
      while (fileIterator.hasNext()) {
        pathSet.add(fileIterator.next());
      }

      return pathSet;
    }
  }
}

