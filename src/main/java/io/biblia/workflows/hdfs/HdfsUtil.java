package io.biblia.workflows.hdfs;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.FileUtil;

import com.google.common.base.Preconditions;

public class HdfsUtil {

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
}
