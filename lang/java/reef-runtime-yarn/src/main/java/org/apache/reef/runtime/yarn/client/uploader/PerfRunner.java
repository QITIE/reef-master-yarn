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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PerfRunner implements Runnable{
    static String  defaultFS;
    static int  blockSizeMB;
    static long  totalUploadMB;
    static int nThread;
    static String jobId;
    static FileSystem fs;
    
    int runId;
    
    static BlockingQueue<Long> latencies = new LinkedBlockingDeque<Long>();
    static BlockingQueue<Long> retries = new LinkedBlockingDeque<Long>();

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
        
        /*
        if (Files.exists(Paths.get("./YarnppLogging.dll")))
        {
        	System.out.println("YarnppLogging.dll exists");
        }
        
        if (Files.exists(Paths.get("./cosmosfs.dll")))
        {
        	System.out.println("cosmosfs.dll exists");
        }
        
        if (Files.exists(Paths.get("./SecureStoreLibraryWrapper.dll")))
        {
        	System.out.println("SecureStoreLibraryWrapper.dll exists");
        }
        
        if (Files.exists(Paths.get("./SecureStoreLibrary.dll")))
        {
        	System.out.println("SecureStoreLibrary.dll exists");
        }
        
        if (Files.exists(Paths.get("./HDFSShim.ini")))
        {
        	System.out.println("HDFSShim.ini exists");
        }
        
        if (Files.exists(Paths.get("./cosmos.ini")))
        {
        	System.out.println("cosmos.ini exists");
        }
        */
        
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
        
        Thread[] threads = new Thread[nThread];
        
        for(int i = 0; i< nThread; i++)
        {
            threads[i] = new Thread(new PerfRunner(i*2));
            threads[i].start();
        }
        
        for(int i = 0; i< nThread; i++)
        {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        String computerName = System.getenv("COMPUTERNAME");
        String fileName = computerName + ".perf";
        try {
			FSDataOutputStream out = fs.create(new Path(defaultFS + "/latency/" + jobId + fileName), true, blockSizeMB * 1000 * 1000);
	        for(Iterator<Long> i=latencies.iterator(); i.hasNext();)
	        {
	        	String newLine = "" + i.next() + "\n";
	        	out.write(newLine.getBytes("utf-8"));
	        }
	        out.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public PerfRunner(Object parameter)
    {
    	runId = (int)parameter;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub        
        String computerName = System.getenv("COMPUTERNAME");
        String fileName = computerName + "_" + "part_" + runId;
        String fileName2 = computerName + "_" + "part_" + (runId+1);

        int maxTries = 10, _trycount=0;
        
        try {
            FSDataOutputStream out = fs.create(new Path(defaultFS + "/upload/" + fileName), true, blockSizeMB * 1000 * 1000);
            FSDataOutputStream out2 = fs.create(new Path(defaultFS + "/upload/" + fileName2), true, blockSizeMB * 1000 * 1000);

            int loops = (int)((totalUploadMB / (2*nThread)) / blockSizeMB);
            byte[] b = new byte[blockSizeMB * 1000 * 1000];
            Random rand = new Random();
            rand.nextBytes(b);
            for (int i=0; i <  loops; i++)
            {
            	RetryWriteBytes(maxTries, _trycount, out, b);
            	RetryWriteBytes(maxTries, _trycount, out2, b);
            }
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println("fail to append data file system");
            e.printStackTrace();
        }
    }

	private void RetryWriteBytes(int maxTries, int _trycount, FSDataOutputStream out, byte[] b) {
		boolean retry = false;
		do
		{
			retry = false;
			long t1 = System.nanoTime();
			try{
				out.write(b);
				long t2 = System.nanoTime();
				
				latencies.add(t2-t1);
			}
			catch(IOException e)
			{
				long t2 = System.nanoTime();
				retries.add(t2-t1);
				
				System.out.println("fail to append data file system, retry after 500ms");
				e.printStackTrace();
				retry = true;
		    	try {
					Thread.sleep(500);
				} catch (InterruptedException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
			}
		}while(retry && ++_trycount<maxTries);
	}
}