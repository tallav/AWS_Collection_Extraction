package aws_ass2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.util.NumberUtils;

import java.util.Iterator;

// counts total number of words
public class Step1 {
	
	public static String inputPath;
	public static String outputPath;
	
	/*
	 * Input:
	 * 		key				value
	 * 		word;decade		totalOccurrences
	 * Output:
	 * 		*				totalOccurrences
	 */
    private static class Map extends Mapper<Text, Text, Text, Text> {
    	@Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String word = key.toString();
            if (!word.equals("*")) {
                context.write(new Text("*"), value); // map * and the number of occurrences. used to count total number of words in the corpus.
            }
        }
    }

    /*
	 * Input:
	 * 		key		value
	 * 		*		totalOccurrences
	 * Output:
	 * 		*		corpusSize
	 */
    private static class Reduce extends Reducer<Text, Text, Text, Text> {
    	@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalNumOfOccur = 0;
            Iterator<Text> t = values.iterator();
            for (Text value : values) {
                totalNumOfOccur = totalNumOfOccur + Long.parseLong(value.toString());
            }
            Text numOfOccur = new Text(String.format("%s", totalNumOfOccur));
            context.write(key, numOfOccur);
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
    	inputPath = args[2];
    	outputPath = args[3];
    	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step1.class);
		job.setMapperClass(Step1.Map.class);
		job.setPartitionerClass(Step1.PartitionerClass.class);
		job.setCombinerClass(Step1.Reduce.class);
		job.setReducerClass(Step1.Reduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
    }
    
}