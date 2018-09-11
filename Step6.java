package aws_ass2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.util.NumberUtils;

public class Step6 {

	public static String inputPath;
	public static String outputPath;
	// the number of reducer tasks, for every decade there will be a separate output
	public static int firstDecade = 0;
	public static int numPartitions = 202;
	
	/*		key          | value
	 * input:
	 * 		w1 w2;decade  |  c12    prob
	 *output:
	 *		decade;prob  |  w1 w2
	 */
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String _key = key.toString();
            int lastInd = _key.lastIndexOf(";");
            String decade = _key.substring(lastInd+1, _key.length());
            String words = _key.substring(0, lastInd);
            String[] splittedVal = value.toString().split("\t");
            //String occur = splittedVal[0];
            String prob = splittedVal[1];
            Text newKey = new Text(decade + ";" + prob);
            Text newVal = new Text(words);
            context.write(newKey, newVal);
        }
    }

    /*		key          | value
     * input:
     * 		decade;prob  |  w1 w2
     * output:
     * 		decade;prob  |  w1 w2 (in decreasing order by prob)
     */
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	for(Text val : values){
                context.write(key,val);
    		}
    	}
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
    	@Override
    	public int getPartition(Text key, Text value, int numPartitions) {
            String _key = key.toString();
            int lastInd = _key.lastIndexOf(";");
            String decadeSt = _key.substring(lastInd + 1, _key.length());
            Integer decade = NumberUtils.tryParseInt(decadeSt);
            if(decade != null)
                return decade % numPartitions;
            else {
                return 0;
            }
        }
    }

    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable  w1, WritableComparable  w2) {
        	String key1 = w1.toString();
        	String key2 = w2.toString();
            int lastInd1 = key1.lastIndexOf(";");
            int lastInd2 = key2.lastIndexOf(";");
            String decade1st = key1.substring(0, lastInd1);
            String prob1st = key1.substring(lastInd1+1, key1.length());
            String decade2st = key2.substring(0, lastInd2);
            String prob2st = key2.substring(lastInd2+1, key2.length());

            Integer decade1 = 0, decade2 = 0;
            Double prob1 = 0.0, prob2 = 0.0;
            try {
            	decade1 = Integer.parseInt(decade1st);
            	decade2 = Integer.parseInt(decade2st);
            	prob1 = Double.parseDouble(prob1st);
            	prob2 = Double.parseDouble(prob2st);
            }
            catch (NumberFormatException ex){}
            int returnVal = decade1.compareTo(decade2);
            if(returnVal == 0){
            	returnVal = -(prob1.compareTo(prob2));
            }
            return returnVal;
        }
    }
    
    public static void main(String[] args) throws Exception {
        inputPath = args[2]; 
        outputPath = args[3];
        
        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step6.class);
        job.setSortComparatorClass(Step6.DescendingKeyComparator.class);
		job.setMapperClass(Step6.MapperClass.class);
		job.setPartitionerClass(Step6.PartitionerClass.class);
		//job.setCombinerClass(Step6.ReducerClass.class);
		job.setReducerClass(Step6.ReducerClass.class);
		job.setNumReduceTasks(numPartitions);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
    }
    
}