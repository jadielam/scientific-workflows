package io.biblia.workflows.hdfs;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.OutputStream;
import java.net.URL;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import java.net.MalformedURLException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.ConfigurationKeys;

public class HdfsUtil implements ConfigurationKeys {

	private static FileSystem fs;
	
	private static String NAMENODE_URL;
	
	final static Logger logger = Logger.getLogger(HdfsUtil.class.getName());
	
	static
	{
		
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		logger.log(Level.FINE, "Changed the URL stream handler factory to recognize hdfs");
		NAMENODE_URL = io.biblia.workflows.Configuration.getValue(NAMENODE);
		
		try{
			Configuration conf = new Configuration();
		    conf.set("fs.hdfs.impl", 
		            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		        );
		    conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
			conf.set("fs.defaultFS", NAMENODE_URL);
			fs = FileSystem.get(new URI(NAMENODE_URL), conf);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	/**
	 * Removes a path. 
	 * If the path is a folder, it removes the folder and everything in it.
	 * If it is a file, it removes the file.
	 * @param path The path does not include the base url of the namenode.
	 */
	public static void deletePath(String path) throws IOException {
		Preconditions.checkNotNull(path);
		File dfsFile = new File(path);
		if (dfsFile.exists()) {
			
			//If it is a directory removes the directory and everything in it.
			//If it is a simple file, removes the file.
			boolean deleted = FileUtil.fullyDelete(dfsFile);
			if (!deleted) {
				throw new IOException("Path "+ path + " could not be deleted");
			}
		}
	}
	
	public static void writeStringToFile(String text, String folderPath, String fileName) throws IllegalArgumentException, IOException {

		// 2. Create outputStream with correct path
		String workflowPath = combinePath(folderPath, fileName);
		Path file = new Path(workflowPath);
		if (fs.exists(file)) {
			fs.delete(file, true);
		}
		OutputStream out = fs.create(file);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
		br.write(text);
		br.close();
		
	}
	
	/**
	 * 
	 * Combines the hdfs paths into one.
	 */
	public static String combinePath(String basePath, String relativePath) 
		throws MalformedURLException 
	{
		Preconditions.checkNotNull(basePath);
		Preconditions.checkNotNull(relativePath);
		basePath = basePath + "/";
		//URL mergedURL = new URL(new URL(basePath), relativePath);
		//return mergedURL.toString();
		return basePath + relativePath;
	}
	
	/**
	 * Returns the size of the path specified by path.
	 * Note that the size already factors in replication.
	 * If the path is a file, returns the size of the file
	 * If the path is a folder, returns the size of the folder, calculating
	 * it recursively.
	 * If the path does not exists, it returns null.
	 * @param path
	 * @return the size of the path in MB.
	 * @throws IOException
	 */
	public static Double getSizeInMB(String filename) throws IOException {
		Preconditions.checkNotNull(filename);
		Long bytes = getSizeInBytes(filename);
		if (null == bytes) {
			return null;
		}
		return new Double(bytes / 1000000.0);
	}
	
	/**
	 * Returns the size in bytes.
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public static Long getSizeInBytes(String filename) throws IOException {
		Preconditions.checkNotNull(filename);
		Path filenamePath = new Path(filename);
		if (fs.exists(filenamePath)) {
			long bytes = fs.getContentSummary(filenamePath).getSpaceConsumed();
			return Long.valueOf(bytes);
		}
		else {
			return null;
		}
		
	}
	
	public static Long getFileSystemCapacityInMB() throws IOException {
		FsStatus status = fs.getStatus();
		long capacity = status.getCapacity();
		return new Long(capacity / 1000000);
	}
	
	public static Long getFileSystemUsedSpaceInMB() throws IOException {
		FsStatus status = fs.getStatus();
		long usedSpace = status.getUsed();
		return new Long(usedSpace / 1000000);
	}
	
	public static Long getFileSystemRemainingSpaceInMB() throws IOException {
		FsStatus status = fs.getStatus();
		long remainingSpace = status.getRemaining();
		return new Long(remainingSpace / 1000000);
	}
}
