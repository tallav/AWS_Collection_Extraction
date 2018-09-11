package aws_ass2;

import java.io.IOException;
import com.amazonaws.util.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * In this step we delete all the stop words from the 1-gram corpus
 * so they won't count in our probability calculations
 * also we count all the 1-gram occurrences for every decade (not including stop words)
 */
public class Step0 {
	
	public static String unigramInputPath;
	public static String stopWordsPath;
	public static String outputPath;
	// the number of reducer tasks, for every decade there will be a separate output
	public static int firstDecade = 0;
	public static int numPartitions = 202;
	
	/*
	 * Input:
	 * 		key		value
	 * 		line	1-gram	year	occurrences
	 * 		line	stopWord
	 * Output:
	 * 		key				value
	 * 		word;decade		occurrences
	 * 		word;decade		Stop
	 */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
    	@Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            String[] strings = value.toString().split("\t");
			if (strings.length == 0) return;
			String word = strings[0].trim().toLowerCase();
            if (strings.length == 1){ // in case that the input is the file that contains all stop words
            	for(int i = firstDecade; i < firstDecade+numPartitions; i++){
            		context.write(new Text(word + ";" + i), new Text("Stop"));
            	}
            } else if (strings.length > 2){
            	String decade = strings[1].substring(0,strings[1].length()-1);
            	Text newKey = new Text(word + ";" + decade);
                Text newValue = new Text(strings[2]); // saves the year and number of occurrences
                context.write(newKey, newValue); // map the word (key) with the year and number of occurrences (value)
            }
        }
    }
    
    /*
	 * Input:
	 * 		key				value
	 * 		word;decade		occurrences
	 * 		word;decade		Stop
	 * Output:
	 * 		key				value
	 * 		word;decade		totalOccurrences
	 * 		(stop words are removed)
	 */
    private static class Reduce extends Reducer<Text, Text, Text, Text> {
    	@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalNumOfOccur = 0;
            boolean isStopWord = false;
            for (Text value : values) { 
                if (!value.toString().equals("Stop")){
                  	totalNumOfOccur += Long.parseLong(value.toString());
                } else {
                	isStopWord = true;
                }
            }
            if (totalNumOfOccur != 0 && !isStopWord){	
                context.write(key, new Text(String.format("%s",totalNumOfOccur)));
            }
        }
    }
    
    private static class PartitionerClass extends Partitioner<Text, Text> {
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

    public static void main(String[] args) throws Exception {
    	unigramInputPath =args[2];
    	stopWordsPath = args[3];
    	outputPath = args[4];

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step0.class);
		job.setMapperClass(Step0.Map.class);
		job.setPartitionerClass(Step0.PartitionerClass.class);
		job.setCombinerClass(Step0.Reduce.class);
		job.setReducerClass(Step0.Reduce.class);
		job.setNumReduceTasks(numPartitions);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(unigramInputPath), SequenceFileInputFormat.class);
		//MultipleInputs.addInputPath(job, new Path(unigramInputPath), TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(stopWordsPath), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
    }  
    
}