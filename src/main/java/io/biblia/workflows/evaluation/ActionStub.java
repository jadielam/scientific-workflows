package io.biblia.workflows.evaluation;

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

	public static void main(String[] args) {
		
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
	 */ 
	private void writeDataset(String filePath, int size) {
		
		//1. Write bogus data into that file until size in MBs is satisfied.
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		OutputStream out = fs.create(new Path(dst));
		IOUtils.copyBytes("testing", out, 4096, true);
		
	}
	
	private void compute(long milliseconds) {
		Preconditions.checkArgument(milliseconds > 0);
		try {
			Thread.sleep(milliseconds);
		}
		catch(InterruptedException ex) {
			System.err.println("ERROR: Current thread interrupted while sleeping for " + milliseconds + " milliseconds");
			Thread.currentThread().interrupt();
		}
	}
}
