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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step5 {
	
	public static String sizePath;
	public static String inputPath;
	public static String outputPath;
	public static Long CORPUS_SIZE = new Long(0);
	// the number of reducer tasks, for every decade there will be a separate output
	public static int firstDecade = 0;
	public static int numPartitions = 202;
	
	/*      key        | value
    input:
          w1 w2;decade | w1	c1	c12	 w2	c2
          *			   | corpusSize (input from step 1)
    output:
          w1 w2;decade | w1	c1	c12	 w2	c2
     */
	public static class MapperClass extends Mapper<Text, Text, Text, Text>{
	   @Override
	   public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
	      if (!key.toString().equals("*")){
	         context.write(key, value);
	      }
	      else{
	    	  CORPUS_SIZE = Long.parseLong(value.toString());
	      }
	   }
	}
	
	/*
	 * The input is the output of Step4
	 * The reduce function will calculate the log likelihood ratio.
	 * Input:
	 * 		key				value
	 * 		w1 w2;decade	w1	c1	c12	w2	c2
	 * Output:
	 * 		key				value
	 *		w1 w2;decade	c12	 log_likelihood_ratio
	 */
	public static class ReducerClass extends Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
			Double N = CORPUS_SIZE.doubleValue();
			for (Text value : values) {
				String[] splitVal = value.toString().split("\t");
				if(splitVal.length > 4){
					double c12 = Double.parseDouble(splitVal[2]);
					double c1 = Double.parseDouble(splitVal[1]);
					double c2 = Double.parseDouble(splitVal[4]);
					double p = c2/N;
					double p1 = c12/c1;
					double p2 = (c2-c12)/(N-c1);
					double L1 = calculateL(c12, c1, p);
					double L2 = calculateL(c2-c12, N-c1, p);
					double L3 = calculateL(c12, c1, p1);
					double L4 = calculateL(c2-c12, N-c1, p2);
					Double val = new Double(Math.log(L1) + Math.log(L2) - Math.log(L3) - Math.log(L4));
					Double c12d = new Double(c12);
					//if(!Double.isNaN(val)){
						Text newVal = new Text(c12d.toString() + "\t" + val.toString());
						context.write(key, newVal);
					//}
				}
			}
		}
	}
	
	private static double calculateL(double k, double n, double x){
		double a = 1-x;
		double b = n-k;
		double c = Math.pow(a, b);
		double d = Math.pow(x, k);
		return c*d;
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
		sizePath = args[4]; 
		inputPath = args[2]; 
		outputPath = args[3];

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step5.class);
		job.setMapperClass(Step5.MapperClass.class);
		job.setPartitionerClass(Step5.PartitionerClass.class);
		//job.setCombinerClass(Step5.ReducerClass.class);
		job.setReducerClass(Step5.ReducerClass.class);
		job.setNumReduceTasks(numPartitions);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(sizePath), KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(inputPath), KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
}