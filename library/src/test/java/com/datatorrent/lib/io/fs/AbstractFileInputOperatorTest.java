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
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.*;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator.DirectoryScanner;
import com.datatorrent.lib.partitioner.StatelessPartitionerTest.PartitioningContextImpl;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.*;

import java.io.*;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.junit.*;
import org.junit.rules.TestWatcher;

public class AbstractFileInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "FileInputOperatorTest");
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
    }
  }

  @Rule public TestMeta testMeta = new TestMeta();

  public static class TestFileInputOperator extends AbstractFileInputOperator<String>
  {
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
    private transient BufferedReader br = null;

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));
      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      super.closeFile(is);
      br.close();
      br = null;
    }

    @Override
    protected String readEntity() throws IOException
    {
      return br.readLine();
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple);
    }
  }

  @Test
  public void testSinglePartiton() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    HashSet<String> allLines = Sets.newHashSet();
    for (int file=0; file<2; file++) {
      HashSet<String> lines = Sets.newHashSet();
      for (int line=0; line<2; line++) {
        lines.add("f"+file+"l"+line);
      }
      allLines.addAll(lines);
      FileUtils.write(new File(testMeta.dir, "file"+file), StringUtils.join(lines, '\n'));
    }

    TestFileInputOperator oper = new TestFileInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(testMeta.context);
    for (long wid=0; wid<3; wid++) {
      oper.beginWindow(wid);
      oper.emitTuples();
      oper.endWindow();
    }
    oper.teardown();

    Assert.assertEquals("number tuples", 4, queryResults.collectedTuples.size());
    Assert.assertEquals("lines", allLines, new HashSet<String>(queryResults.collectedTuples));

  }

  @Test
  public void testScannerPartitioning() throws Exception
  {
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setFilePatternRegexp(".*partition([\\d]*)");

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "");
    }

    FileSystem fs = FileSystem.get(FileContext.getLocalFSFileContext().getDefaultFileSystem().getUri(), new Configuration());
    List<DirectoryScanner> partitions = scanner.partition(2);
    Set<Path> allFiles = Sets.newHashSet();
    for (DirectoryScanner partition : partitions) {
      Set<Path> files = partition.scan(fs, path, Sets.<String>newHashSet());
      Assert.assertEquals("", 2, files.size());
      allFiles.addAll(files);
    }
    Assert.assertEquals("Found all files " + allFiles, 4, allFiles.size());

  }

  @Test
  public void testPartitioning() throws Exception
  {
    TestFileInputOperator oper = new TestFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    for (int file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "");
    }

    List<Partition<AbstractFileInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFileInputOperator<String>>(oper));
    Collection<Partition<AbstractFileInputOperator<String>>> newPartitions = oper.definePartitions(partitions, new PartitioningContextImpl(null, 2));
    Assert.assertEquals(2, newPartitions.size());
    Assert.assertEquals(1, oper.getCurrentPartitions()); // partitioned() wasn't called

    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      Assert.assertNotSame(oper, p.getPartitionedInstance());
      Assert.assertNotSame(oper.getScanner(), p.getPartitionedInstance().getScanner());
      Set<String> consumed = Sets.newHashSet();
      LinkedHashSet<Path> files = p.getPartitionedInstance().getScanner().scan(FileSystem.getLocal(new Configuration(false)), path, consumed);
      Assert.assertEquals("partition " + files, 2, files.size());
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

    TestFileInputOperator oper = new TestFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);

    TestFileInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    // Read all records to populate processedList in operator.
    oper.setup(testMeta.context);
    for(int i = 0; i < 10; i++) {
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
      TestFileInputOperator oi = (TestFileInputOperator)p.getPartitionedInstance();
      oi.setup(testMeta.context);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFileInputOperator<String> o : opers) {
        o.beginWindow(wid);
        o.emitTuples();
        o.endWindow();
      }
      wid++;
    }

    // No record should be read.
    Assert.assertEquals("No new tuples read ", 0, sink.collectedTuples.size());

    // Add four new files with 3 records each.
    for (; file<8; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    for(int i = 0; i < 10; i++) {
      for(AbstractFileInputOperator<String> o : opers) {
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
    TestFileInputOperator oper = new TestFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);
    oper.setEmitBatchSize(2);

    TestFileInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    //Read some records
    oper.setup(testMeta.context);
    for(int i = 0; i < 5; i++) {
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
      TestFileInputOperator oi = (TestFileInputOperator)p.getPartitionedInstance();
      oi.setup(testMeta.context);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFileInputOperator<String> o : opers) {
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
    TestFileInputOperator oper = new TestFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setScanIntervalMillis(0);
    oper.setEmitBatchSize(2);

    TestFileInputOperator initialState = new Kryo().copy(oper);

    // Create 4 files with 3 records each.
    Path path = new Path(new File(testMeta.dir).getAbsolutePath());
    FileContext.getLocalFSFileContext().delete(path, true);
    int file;
    for (file=0; file<4; file++) {
      FileUtils.write(new File(testMeta.dir, "partition00"+file), "a\nb\nc\n");
    }

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    int wid = 0;

    //Read some records
    oper.setup(testMeta.context);
    for(int i = 0; i < 5; i++) {
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
      TestFileInputOperator oi = (TestFileInputOperator)p.getPartitionedInstance();
      oi.setup(testMeta.context);
      oi.output.setSink(sink);
      opers.add(oi);
    }

    sink.clear();
    for(int i = 0; i < 10; i++) {
      for(AbstractFileInputOperator<String> o : opers) {
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


    TestFileInputOperator oper = new TestFileInputOperator();
    oper.scanner = null;
    oper.failedFiles.add(new AbstractFileInputOperator.FailedFile(testFile.getAbsolutePath(), 1));

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
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

    TestFileInputOperator oper = new TestFileInputOperator();
    oper.scanner = null;
    oper.unfinishedFiles.add(new AbstractFileInputOperator.FailedFile(testFile.getAbsolutePath(), 2));

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
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

    TestFileInputOperator oper = new TestFileInputOperator();
    oper.scanner = null;
    oper.pendingFiles.add(testFile.getAbsolutePath());

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
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

    TestFileInputOperator oper = new TestFileInputOperator();
    oper.scanner = null;
    oper.currentFile = testFile.getAbsolutePath();
    oper.offset = 1;

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
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

    TestFileInputOperator oper = new TestFileInputOperator();
    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    manager.setRecoveryPath(testMeta.dir + "/recovery");

    oper.setIdempotentStorageManager(manager);

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

    TestFileInputOperator oper = new TestFileInputOperator();
    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    manager.setRecoveryPath(testMeta.dir + "/recovery");

    oper.setIdempotentStorageManager(manager);

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

    TestFileInputOperator oper = new TestFileInputOperator();
    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    manager.setRecoveryPath(testMeta.dir + "/recovery");
    oper.setEmitBatchSize(5);

    oper.setIdempotentStorageManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
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

    TestFileInputOperator oper = new TestFileInputOperator();

    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    manager.setRecoveryPath(testMeta.dir + "/recovery");

    oper.setIdempotentStorageManager(manager);

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
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
  public void testIdempotentStorageManagerPartitioning() throws Exception
  {
    TestFileInputOperator oper = new TestFileInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());
    oper.setIdempotentStorageManager(new TestStorageManager());
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

    List<TestStorageManager> storageManagers = Lists.newLinkedList();
    for (Partition<AbstractFileInputOperator<String>> p : newPartitions) {
      storageManagers.add((TestStorageManager) p.getPartitionedInstance().idempotentStorageManager);
    }
    Assert.assertEquals("count of storage managers", 2, storageManagers.size());

    int countOfDeleteManagers = 0;
    TestStorageManager deleteManager = null;
    for (TestStorageManager storageManager : storageManagers) {
      if (storageManager.getDeletedOperators() != null) {
        countOfDeleteManagers++;
        deleteManager = storageManager;
      }
    }

    Assert.assertEquals("count of delete managers", 1, countOfDeleteManagers);
    Assert.assertNotNull("deleted operators manager", deleteManager);
    Assert.assertEquals("deleted operators", Sets.newHashSet(7), deleteManager.getDeletedOperators());
  }

  private static class TestStorageManager extends IdempotentStorageManager.FSIdempotentStorageManager
  {
    Set<Integer> getDeletedOperators()
    {
      if (deletedOperators != null) {
        return ImmutableSet.copyOf(deletedOperators);
      }
      return null;
    }
  }
}
