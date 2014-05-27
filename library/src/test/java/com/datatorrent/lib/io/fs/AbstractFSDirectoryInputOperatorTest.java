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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.python.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.io.fs.AbstractFSDirectoryInputOperator.DirectoryScanner;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Sets;

public class AbstractFSDirectoryInputOperatorTest
{
  public static class TestMeta extends TestWatcher
  {
    public String dir = null;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
    }
  };

  @Rule public TestMeta testMeta = new TestMeta();

  public static class TestFSDirectoryInputOperator extends AbstractFSDirectoryInputOperator<String>
  {
    @OutputPortFieldAnnotation(name = "output")
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
  public void test() throws Exception
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

    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();

    CollectorTestSink<String> queryResults = new CollectorTestSink<String>();
    @SuppressWarnings({ "unchecked", "rawtypes" })
    CollectorTestSink<Object> sink = (CollectorTestSink) queryResults;
    oper.output.setSink(sink);

    oper.setDirectory(testMeta.dir);
    oper.getScanner().setFilePatternRegexp(".*file[\\d]");

    oper.setup(null);
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
    TestFSDirectoryInputOperator oper = new TestFSDirectoryInputOperator();
    oper.getScanner().setFilePatternRegexp(".*partition([\\d]*)");
    oper.setDirectory(new File(testMeta.dir).getAbsolutePath());

    List<Partition<AbstractFSDirectoryInputOperator<String>>> partitions = Lists.newArrayList();
    partitions.add(new DefaultPartition<AbstractFSDirectoryInputOperator<String>>(oper));
    Collection<Partition<AbstractFSDirectoryInputOperator<String>>> newPartitions = oper.definePartitions(partitions, 2);
    Assert.assertEquals(3, newPartitions.size());

    for (Partition<AbstractFSDirectoryInputOperator<String>> p : newPartitions) {
      Assert.assertNotSame(oper, p.getPartitionedInstance());
      Assert.assertNotSame(oper.getScanner(), p.getPartitionedInstance().getScanner());
    }

  }

}
