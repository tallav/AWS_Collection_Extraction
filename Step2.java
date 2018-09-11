package aws_ass2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.util.NumberUtils;

/*
 * This step counts all the 2-gram in the corpus
 */
public class Step2 {
	
	public static String bigramInputPath;
	public static String outputPath;
	public static String yearsCountPath;
	// the number of reducer tasks, for every decade there will be a separate output
	public static int firstDecade = 0;
	public static int numPartitions = 202;
	
	/* 
	 * The value is a tab separated string containing the following fields:
	 * 2-gram, year, occurrences, pages, books 
	 * Input:
	 * 		key		value
	 * 		line	w1 w2	year	occurrences
	 * Output:
	 * 		key				value
	 * 		w1 w2;decade	occurrences
	 */
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			String[] strings = value.toString().split("\t");
			if (strings.length == 0) return;
			if(strings.length > 2){
				String bigram = strings[0].toLowerCase();
				String decade = strings[1].substring(0,strings[1].length()-1);
				String occur = strings[2];
				Text newKey = new Text(bigram + ";" + decade);
				Text newVal = new Text(occur);
				context.write(newKey, newVal);
			}
		}
	}
	
	/* 
	 * Input:
	 * 		key				value
	 * 		w1 w2;decade	occurrences
	 * Output:
	 * 		key				value
	 * 		w1 w2;decade	toatalOccurrences
	 */
	public static class ReducerClass extends Reducer<Text,Text,Text,Text>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			int totalNumOfOccur = 0;
			for (Text value : values) {
				totalNumOfOccur += Long.parseLong(value.toString());
			}
			context.write(key, new Text(String.format("%s",totalNumOfOccur))); 
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
	
	public static void main(String[] args) throws Exception {
		bigramInputPath = args[2];
    	outputPath = args[3];
    	
    	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step2.class);
		job.setMapperClass(Step2.MapperClass.class);
		job.setPartitionerClass(Step2.PartitionerClass.class);
		job.setCombinerClass(Step2.ReducerClass.class);
		job.setReducerClass(Step2.ReducerClass.class);
		job.setNumReduceTasks(numPartitions);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(bigramInputPath), SequenceFileInputFormat.class);
		//MultipleInputs.addInputPath(job, new Path(bigramInputPath), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
}