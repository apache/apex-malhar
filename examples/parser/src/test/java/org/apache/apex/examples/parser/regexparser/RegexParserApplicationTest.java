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
package org.apache.apex.examples.parser.regexparser;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.StramLocalCluster;

public class RegexParserApplicationTest
{

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/properties-regexParserApplication.xml"));
      conf.setLong("dt.application.RegexParser.operator.logGenerator.prop.tupleRate", 10);
      final String dataFolderPath = conf.get("dt.application.RegexParser.operator.*.prop.filePath");
      final String dataFileName = conf
          .get("dt.application.RegexParser.operator.regexWriter.prop.outputFileName");

      FileUtils.deleteDirectory(new File(dataFolderPath));
      lma.prepareDAG(new RegexParserApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.setHeartbeatMonitoringEnabled(false);
      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          if (new File(dataFolderPath).exists()) {
            Collection<File> files = FileUtils.listFiles(new File(dataFolderPath), new WildcardFileFilter(dataFileName
                + "*"), null);
            if (files.size() >= 1) {
              File parsedFile = files.iterator().next();
              String fileData = FileUtils.readFileToString(parsedFile);
              String[] regexData = fileData.split("\n");
              return regexData.length == 10;
            }
          }
          return false;
        }
      });

      lc.run(30 * 1000); // runs for 30 seconds and quitxs

      Collection<File> files = FileUtils.listFiles(new File(dataFolderPath),
          new WildcardFileFilter(dataFileName + "*"), null);
      File parsedFile = files.iterator().next();
      String fileData = FileUtils.readFileToString(parsedFile);
      String[] logData = fileData.split("\n");
      for (String logLine : logData) {
        Assert.assertTrue(logLine.contains("id=" + 101));
        Assert.assertTrue(logLine.contains("signInId=" + "'11111@psop.com'"));
        Assert.assertTrue(logLine.contains("serviceId=" + "'IP1234-NPB12345_00'"));
        Assert.assertTrue(logLine.contains("accountId=" + "'11111'"));
        Assert.assertTrue(logLine.contains("platform=" + "'pik'"));
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
