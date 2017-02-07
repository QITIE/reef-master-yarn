/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client.uploader;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CleanRunner{
    static String  defaultFS;
    static int  blockSizeMB;
    static long  totalUploadMB;
    static int nThread;
    static String jobId;
    static FileSystem fs;
    
    int runId;

    public static void main(String[] args)  {
        // TODO Auto-generated method stub
        Map<String, String> s = System.getenv();
        
        for (Iterator i = s.keySet().iterator(); i.hasNext(); )
        {
            String key = (String) i.next();
            String value = (String) s.get(key);
        	System.out.println("Java Env:" + key + " = " + value);
        }
        
        System.out.println("done enum env var");
        
        defaultFS = args[0];
        blockSizeMB = Integer.parseInt(args[1]);
        totalUploadMB = Integer.parseInt(args[2]);
        nThread = Integer.parseInt(args[3]);
        jobId = args[4];
        System.out.println("defaultFS = " + defaultFS);
        System.out.println("blockSizeMB = " + blockSizeMB);
        System.out.println("totalUploadMB = " + totalUploadMB);
        System.out.println("nThread = " + nThread);
        System.out.println("jobId = " + jobId);
        
        YarnConfiguration conf = new YarnConfiguration();
        conf.set("fs.defaultFS", defaultFS);
        
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println("fail to create file system");
            e.printStackTrace();
            return;
        }
        
        boolean needRetry = false;
        int maxretry = 10, retrycount = 0;
        Path uploadFolder = new Path(defaultFS + "/upload/");
        Path downloadFolder = new Path(defaultFS + "/download/");
        
        do{
            needRetry = false;
            try {
            	if (fs.exists(uploadFolder))
            	{
                    fs.delete(uploadFolder, true);
            	}

            	if (fs.exists(downloadFolder))
            	{
                    fs.delete(downloadFolder, true);
            	}
            	
            	fs.mkdirs(uploadFolder);
            	fs.mkdirs(downloadFolder);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                needRetry = true;
            }
        }
        while(++retrycount < maxretry && needRetry);
        
        try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}