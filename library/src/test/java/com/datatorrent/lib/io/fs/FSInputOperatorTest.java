/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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

import com.datatorrent.lib.testbench.CollectorTestSink;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.compressors.*;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FSInputOperatorTest
{
    private static final int NUMBER_OF_TEST_FILES = 100;
    private static final int FILE_NAME_BIT_COUNT = 100;
    private static final int FILE_LINE_BIT_COUNT = 500;
    private static final int FILE_LINE_COUNT = 10;
    private static final int FILE_TUPLE_COUNT = NUMBER_OF_TEST_FILES * FILE_LINE_COUNT;
    
    private static final Random random = new Random();
    private static final String TEST_DIRECTORY = "target" + File.separator + FSInputOperatorTest.class.getName();
    private static final String TEST_BAK_DIRECTORY = TEST_DIRECTORY + ".bak";
    
    private class FSInputOperatorTestOperator extends FSInputOperator<String>
    {
        @Override
        public String readEntityFromReader(BufferedReader reader)
        {
            try
            {
                return reader.readLine();
            }
            catch(IOException e)
            {
                Assert.fail("There was an error reading from test files.");
            }
            
            return null;
        }   
    }
    
    @Before
    public void createGZFilesForTest() throws Exception
    {
        File testDirectory = new File(TEST_DIRECTORY);
        File testBakDirectory = new File(TEST_BAK_DIRECTORY);
        
        FileContext.getLocalFSFileContext().delete(new Path(testDirectory.getAbsolutePath()), true);
        FileContext.getLocalFSFileContext().delete(new Path(testBakDirectory.getAbsolutePath()), true);
        
        testDirectory.mkdirs();
        testBakDirectory.mkdirs();
        
        for(int fileCounter = 0;
            fileCounter < NUMBER_OF_TEST_FILES;
            fileCounter++)
        {
            String fileName = generateRandomString(FILE_NAME_BIT_COUNT) + ".gz";
            File file = new File(testDirectory, fileName);
            
            if(!file.createNewFile())
            {
                fileCounter--;
                continue;
            }
            
            FileOutputStream output = new FileOutputStream(file.getAbsolutePath());
            
            try
            {
                Writer writer = new OutputStreamWriter(new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, output));
                
                try
                {
                    for(int lineCounter = 0;
                        lineCounter < FILE_LINE_COUNT;
                        lineCounter++)
                    {
                        writer.write(generateRandomString(FILE_LINE_BIT_COUNT));
                        
                        if(lineCounter != FILE_LINE_COUNT -1)
                        {
                            writer.write("\n");
                        }
                    }
                }
                finally
                {
                    writer.close();
                }
            }
            finally
            {
                output.close();
            }
        }
    }
    
    @Test
    public void testSinglePartiton() throws Exception
    {
        FSInputOperatorTestOperator testOperator = new FSInputOperatorTestOperator();
        CollectorTestSink<Object> testSink = new CollectorTestSink<Object>();
        
        testOperator.output.setSink(testSink);
        
        testOperator.setDirectory(TEST_DIRECTORY);
        testOperator.setBackupDirectory(TEST_BAK_DIRECTORY);
        
        testOperator.setup(null);
        
        try
        {
            Path filePath = new Path(TEST_DIRECTORY);
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.newInstance(filePath.toUri(), configuration);

            Set<String> scannedFiles = testOperator.scanDirectories(fs, filePath);
            testOperator.getPendingFiles().addAll(scannedFiles);
        }
        catch(IOException ex)
        {
            throw new RuntimeException(ex);
        }
        
        for(long wid = 0; wid < NUMBER_OF_TEST_FILES; wid++)
        {
            testOperator.beginWindow(wid);
            testOperator.emitTuples();
            testOperator.endWindow();
        }
        
        testOperator.teardown();

        Assert.assertEquals("number tuples", NUMBER_OF_TEST_FILES * FILE_LINE_COUNT, testSink.collectedTuples.size());
        //Assert.assertEquals("lines", allLines, new HashSet<String>(queryResults.collectedTuples));

    }
    
    private String generateRandomString(int bitCount)
    {
        return new BigInteger(bitCount, random).toString(32);
    }
}
