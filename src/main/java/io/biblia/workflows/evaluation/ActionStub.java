package io.biblia.workflows.evaluation;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import io.biblia.workflows.action.Action;
import com.google.common.base.Preconditions;

/**
 * This class it is used as an action stub.
 * For the purpose of evaluating the workflow system, 
 * an action is something that takes computational time 
 * and produces an output of a given size. 
 * 
 * This action stub will receive three parameters:
 * args[0]: path to input file
 * args[1]: path to output file.
 * args[2]: size of output file to produce in MBs
 * args[3]: computational time to take. 
 * @author dearj019
 *
 */
public class ActionStub implements Action {

	public static void main(String[] args) throws IOException {
		
		if (args.length != 4) {
			//Passing the wrong number of parameters.
			System.err.println("ERROR: Not enough number of parameters were passed in args");
			System.exit(1);
		}
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		
		int outputSizeMB = 0;
		try{
			outputSizeMB = Integer.parseInt(args[2]);
		}
		catch(NumberFormatException ex) {
			System.err.println("ERROR: Did not contain a valid integer "
					+ "for parameter representing the size of output file in MBs");
			throw ex;
		}
		
		long computationTimeMilli = 0;
		try {
			computationTimeMilli = Long.parseLong(args[3]);
		}
		catch(NumberFormatException ex) {
			System.err.println("ERROR: Did not contain a valid long for parameter "
					+ "representing the computational time in milliseconds");
			throw ex;
		}
		
		Preconditions.checkArgument(outputSizeMB >= 0, "The output size must be greater than or equal to 0");
		Preconditions.checkArgument(computationTimeMilli > 0, "The computation time in milliseconds needs to be greater than 0");
		ActionStub stub = new ActionStub();
		stub.compute(computationTimeMilli);
		stub.writeDataset(outputFilePath, outputSizeMB);
	}
	
	/**
	 * Writes a file to HDFS with the properties specified by the parameters.
	 * @param filePath: The complete path to the file to write in the hadoop filesystem
	 * @param size: The size of the file to produce in megabytes
	 * @throws IOException 
	 */ 
	private void writeDataset(String filePath, int size) throws IOException {
		Preconditions.checkNotNull(filePath);
		if (size < 0) size = 0;
		long totalBytes = size * 1000000L;
		
		//1. Write bogus data into that file until size in MBs is satisfied.
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		OutputStream out = fs.create(new Path(filePath));
		
		long bytesCopied = 0;
		while (bytesCopied < totalBytes) {
			//Copy 400MB at a time
			int bytesToCopy = (int)Math.min(400000000, Math.max(0, totalBytes - bytesCopied));
			byte [] bytes = new byte[bytesToCopy];
			InputStream is = new ByteArrayInputStream(bytes);
			try{
				IOUtils.copyBytes(is, out, 4096, false);
				is.close();
			}
			catch(IOException e) {
				is.close();
				throw e;
			}
			bytesCopied += bytesToCopy;
		}
		
		out.close();
	}
	
	/**
	 * This function will sleep for the amount of time in milliseconds given
	 * @param milliseconds
	 */
	private void compute(long milliseconds) {
		if (milliseconds < 0) milliseconds = 0;
		try {
			Thread.sleep(milliseconds);
		}
		catch(InterruptedException ex) {
			System.err.println("ERROR: Current thread interrupted while sleeping for " + milliseconds + " milliseconds");
			Thread.currentThread().interrupt();
		}
	}
}
