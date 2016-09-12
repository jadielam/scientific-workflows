package io.biblia.workflows.hdfs;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FsStatus;

import com.google.common.base.Preconditions;

public class HdfsUtil {

	private static FileSystem fs;
	static
	{
		//TODO: Initialize this properly.
		try{
			fs = FileSystem.get(new Configuration());
		}
		catch(IOException ex) {
			//TODO: Log error here, because this here is crazy.
		}
	}
	/**
	 * Removes a path.
	 * If the path is a folder, it removes the folder and everything in it.
	 * If it is a file, it removes the file.
	 * @param path
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
