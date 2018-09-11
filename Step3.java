package aws_ass2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.util.NumberUtils;

public class Step3 {
	
	public static String unigramInputPath;
	public static String bigramInputPath;
	public static String outputPath;
	// the number of reducer tasks, for every decade there will be a separate output
	public static int firstDecade = 0;
	public static int numPartitions = 202;
	
	/*
	 * The input is the output of Step2 - 2-gram /t occurrences
	 * The map function will split the 2-gram to 2 words,
	 * every word with decade is a key and their value will be the 2-gram with his occur (separated by /t).
	 * Input:
	 * 		key				value
	 * 		w1 w2;decade	c12
	 * Output:
	 * 		key				value
	 * 		w1;decade		w1 w2	c12
	 * 		w2;decade		w1 w2	c12	 
	 */
	public static class MapperClass extends Mapper<Text, Text, Text, Text>{
		@Override
		public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
			String ngram = key.toString();
			String occur = value.toString();
			int lastInd = ngram.lastIndexOf(";");
			String decade = ngram.substring(lastInd+1, ngram.length());
			String bigram = ngram.substring(0, lastInd);
			String[] words = bigram.split(" ");
			//split only the keys from step2 output
			//split only the keys from step2 output
			if(words.length > 1){
				String word1 = words[0];
				String word2 = words[1];
				Text w1Key = new Text(word1 + ";" + decade); // word1 + decade
				Text w2Key = new Text(word2 + ";" + decade); // word2 + decade
				Text newVal = new Text(word1+" " +word2 + "\t" + occur);
				context.write(w1Key, newVal);
				context.write(w2Key, newVal);
			} else {
				context.write(key, value);
			}
		}
	}
	
	/*
	 * The input is the output of Step1 and the mapper.
	 * The output has the same key (1-gram;decade) and the value combines the two different types of values from the input
	 * Input:
	 * 		key				value
	 * 		w1;decade		w1 w2	c12
	 * 		w1;decade		c1
	 * 		w2;decade		w1 w2	c12
	 * 		w2;decade		c2
	 * Output:
	 * 		key				value
	 *		w1;decade		w1 w2	c12	c1
	 * 		w2;decade		w1 w2	c12	c2
	 */
	public static class ReducerClass extends Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			String wordOccur = null;
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				String[] splitVal = value.toString().split("\t");
				// a value from the mapper
                if(splitVal.length > 1){
                	String ngramVal = splitVal[0] + "\t" + splitVal[1] + "-";
                	sb.append(ngramVal);
                // a value from the 1-gram	
                } else {
                	wordOccur = splitVal[0];
                }
            }
			String[] bigrams = sb.toString().split("-");
			for(String bigram : bigrams){
				Text newVal = new Text(bigram + "\t" + wordOccur);
				if(wordOccur != null){
					context.write(key, newVal);
				}
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
	
	public static void main(String[] args) throws Exception {
		unigramInputPath = args[2]; 
		bigramInputPath = args[3]; 
		outputPath = args[4];
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step3.class);
		job.setMapperClass(Step3.MapperClass.class);
		job.setPartitionerClass(Step3.PartitionerClass.class);
		//job.setCombinerClass(Step3.ReducerClass.class);
		job.setReducerClass(Step3.ReducerClass.class);
		job.setNumReduceTasks(numPartitions);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(unigramInputPath), KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(bigramInputPath), KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
}