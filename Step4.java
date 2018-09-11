package aws_ass2;

import java.io.IOException;

import com.amazonaws.util.NumberUtils;
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

public class Step4 {
	
	public static String inputPath;
	public static String outputPath;
	// the number of reducer tasks, for every decade there will be a separate output
	public static int firstDecade = 0;
	public static int numPartitions = 202;

	
	/*    key | value
    input:
          w1;decade  | w1w2    c12    c1
          w2;decade  | w1w2    c12    c2
    output:
          w1w2;decade| c12    w1    c1
          w1w2;decade| c12    w2    c2
     */
    private static class Map extends Mapper<Text, Text, Text, Text> {
    	@Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            if(strings.length == 3){
	            String couple = strings[0];
	            String coupleOccur = strings[1];
	            String wordOccur = strings[2];
	            //String[] word = key.toString().split(";");
	            String _key = key.toString();
	            int lastInd = _key.lastIndexOf(";");
	            String decade = _key.substring(lastInd+1, _key.length());
	            String word = _key.substring(0, lastInd);
	            Text newKey = new Text(couple + ";" + decade);
	            Text newVal = new Text(coupleOccur + "\t" + word + "\t" + wordOccur);
	            context.write(newKey, newVal);
            }
        }
    }

    /*      key | value
    input:
          w1w2| c12    w1    c1
          w1w2| c12    w2    c2
    output:
          w1w2| w1	c1	c12	 w2	c2
     */
    private static class Reduce extends Reducer<Text, Text, Text, Text> {
    	@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            int firstWordSize = 0;
            boolean isFirst = true;
            for (Text value : values) {
                if (isFirst){
                    String val = String.valueOf(value);
                    String[] splitedStr = val.split("\t");
                    firstWordSize = splitedStr[0].length(); // keeps the size of the first word
                    isFirst = false;
                }
                sb.append(value + "\t");
            }
            String newValStr = sb.toString();
            newValStr = newValStr.substring(firstWordSize + "\t".length(), newValStr.length()); // delete the first occurence of c12 to avoid duplicate occurences
            Text newVal = new Text(newValStr);
            context.write(key, newVal);
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
        outputPath =args[3];
        
        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step4.class);
		job.setMapperClass(Step4.Map.class);
		job.setPartitionerClass(Step4.PartitionerClass.class);
		//job.setCombinerClass(Step4.ReducerClass.class);
		job.setReducerClass(Step4.Reduce.class);
		job.setNumReduceTasks(numPartitions);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
    }
    
}