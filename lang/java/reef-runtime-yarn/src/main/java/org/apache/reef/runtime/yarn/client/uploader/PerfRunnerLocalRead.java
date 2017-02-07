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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class PerfRunnerLocalRead implements Runnable{
    static String  defaultFS;
    static int  blockSizeMB;
    static long  totalReadMB;
    static int nThread;
    static String jobId;
    static FileSystem fs;
    static BlockingQueue<Long> latencies = new LinkedBlockingDeque<Long>();
    
    String ReadFile;

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
        totalReadMB = Integer.parseInt(args[2]);
        nThread = Integer.parseInt(args[3]);
        jobId = args[4];
        System.out.println("defaultFS = " + defaultFS);
        System.out.println("blockSizeMB = " + blockSizeMB);
        System.out.println("totalReadMB = " + totalReadMB);
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
        
        try {
        	Path configPath = new Path(defaultFS + "/config/BDN2Stream.csv");
			fs.copyToLocalFile(configPath, new Path("./BDN2Stream.csv"));
		} catch (IOException e1) {
            System.out.println("fail to download config BDN2Stream.csv");
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
        
        String[] files = null;
        
        try (BufferedReader br = new BufferedReader(new FileReader("./BDN2Stream.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
               // process the line.
            	if (line.startsWith(System.getenv("COMPUTERNAME").toLowerCase()))
            	{
            		files = line.split(",")[1].split(";");
            	}
            }
        } catch (FileNotFoundException e1) {
            System.out.println("fail to find local config BDN2Stream.csv");
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		} catch (IOException e1) {
            System.out.println("fail to read config BDN2Stream.csv");
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
        
        if (files == null)
        {
            System.out.println("no records about " + System.getenv("COMPUTERNAME").toLowerCase() + " is found in BDN2Stream.csv");
            return;
        }
        
        Random r = new Random();
        
        Thread[] threads = new Thread[nThread];
        
        for(int i = 0; i< nThread; i++)
        {
            threads[i] = new Thread(new PerfRunnerLocalRead(files[r.nextInt(files.length)]));
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
			FSDataOutputStream out = fs.create(new Path(defaultFS + "/latency/" + jobId + "_" + fileName), true, blockSizeMB * 1000 * 1000);
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
    
    public PerfRunnerLocalRead(Object parameter)
    {
    	ReadFile = (String)parameter;
    }

    public void run() {
        // TODO Auto-generated method stub
        try {

            String computerName = System.getenv("COMPUTERNAME");
        	
        	int maxRetries = 100, _trycount = 0;
        	        	
        	System.out.println("download folder opened for read");
        	
        	byte[] buf = new byte[blockSizeMB * 1000 * 1000];
        	double perRunReadMB = (double)totalReadMB / nThread;
        	FSDataInputStream in = null;
        	
        	try
        	{
        		in = fs.open(new Path(ReadFile), blockSizeMB * 1000 * 1000);
        	}
        	catch(IOException e)
        	{
        		System.out.println("fail to open file :" + ReadFile + ", abort");
        		e.printStackTrace();
        		return;
        	}
        	
        	System.out.println(ReadFile + " opened for read");

        	while(perRunReadMB > 0)
        	{
        		in.seek(0); // point to beginning
            	int readBytes=0;
            	do
            	{
        			readBytes = retryRead(buf, in);
        			if(readBytes > 0)
        			{
        				perRunReadMB -= (readBytes / 1000000.0);
                    	System.out.println("read " + readBytes + "bytes from" + ReadFile);
        			}
            	}
            	while(readBytes > 0);
        	}
        }
    	catch(IOException e)
    	{
    		System.out.println("fail to read data from file system, continue" );
    		e.printStackTrace();
    	}
    }
    
    /*
    
    @Override
    public void run() {
        // TODO Auto-generated method stub
        try {

        	Random random = new Random();
        	int skipFactor = random.nextInt(50);
        	int count = 0;
        	RemoteIterator<LocatedFileStatus> files = null;
        	int maxRetries = 100, _trycount = 0;
        	
        	files = RetryOpenFolder(files, maxRetries, _trycount);
        	
        	System.out.println("download folder opened for read");
        	
        	byte[] buf = new byte[blockSizeMB * 1000 * 1000];
        	double perRunReadMB = (double)totalReadMB / nThread;

        	while(perRunReadMB > 0)
        	{
				if (!files.hasNext())
        		{
        			// start another iterator
		        	files = RetryOpenFolder(files, maxRetries, _trycount);
		        }
            	
            	if (++count < skipFactor)
            	{
            		continue;
            	}
            	
            	LocatedFileStatus file = files.next();
            	FSDataInputStream in = null;
            	try
            	{
            		in = fs.open(file.getPath(), blockSizeMB * 1000 * 1000);
            	}
            	catch(IOException e)
            	{
            		System.out.println("fail to open file :" + file.toString() + ", continue");
            		e.printStackTrace();
            		continue;
            	}
            	
            	System.out.println(file.toString() + " opened for read");
            	
            	int readBytes=0;
            	do
            	{
        			readBytes = retryRead(buf, in);
        			if(readBytes > 0)
        			{
        				perRunReadMB -= (readBytes / 1000000.0);
                    	System.out.println("read " + readBytes + "bytes from" + file.toString());
        			}
            	}
            	while(readBytes > 0);
        	}
        }
    	catch(IOException e)
    	{
    		System.out.println("fail to read data from file system, continue" );
    		e.printStackTrace();
    	}
    }*/

	private int retryRead(byte[] buf, FSDataInputStream in) throws IOException {
		int readBytes = -1;
		int _retrycount = 0, maxretry = 10;

		boolean needRetry = false;
		do
		{
			needRetry = false;
			try
			{
				long t1 = System.nanoTime();
				readBytes = in.read(buf);
				long t2 = System.nanoTime();
				
				latencies.add(t2-t1);
			}
			catch(IOException ex)
			{
				needRetry = true;
				ex.printStackTrace();
				if (readBytes < 0)
				{
					in.seek(0);
				}
		    	try {
					Thread.sleep(500);
				} catch (InterruptedException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
			}
		}
		while(needRetry && ++_retrycount < maxretry);
		return readBytes;
	}

	/*
	private RemoteIterator<LocatedFileStatus> RetryOpenFolder(RemoteIterator<LocatedFileStatus> files, int maxRetries,
			int _trycount) {
		boolean needRetry = false;
		do{
			needRetry = false;
			try
			{
				files = fs.listFiles(new Path(defaultFS + "/download/"), false);
			}
			catch(IOException e)
			{
				System.out.println("fail to open download folder, retry after 500ms" );
				e.printStackTrace();
		    	try {
					Thread.sleep(500);
				} catch (InterruptedException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
					needRetry = true;
				}
			}
		}
		while(needRetry && ++_trycount < maxRetries);
		return files;
	}*/
}