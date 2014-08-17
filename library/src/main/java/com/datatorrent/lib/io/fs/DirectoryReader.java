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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class DirectoryReader
{
    private String directory;
    
    protected transient FileSystem fs;
    protected transient Configuration configuration;
    protected transient Path filePath;
    
    public DirectoryReader()
    {
    }
    
    public String getDirectory()
    {
        return directory;
    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
    }
    
    public void directorySetup()
    {
        try
        {
            filePath = new Path(directory);
            configuration = new Configuration();
            fs = FileSystem.newInstance(filePath.toUri(), configuration);
        }
        catch(IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }
    
    public void directoryTearDown()
    {
        try
        {
            fs.close();
        }
        catch(IOException ex)
        {
            //ignore
        }
    }
}
