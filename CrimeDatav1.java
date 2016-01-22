/** Vibha Belavadi
 *  Crime region definition with entire zip-code 
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CrimeDatav1 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String easternRegionCode ="";
		private String northernRegionCode = "";
		private String crimeType = "";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String inp = (value.toString()).trim();
			
			if(!(inp.contains("Crime ID") || inp.contains("Reported by") || inp.contains("Month") || inp.contains("Easting") || inp.contains("Northing") || inp.contains("Falls Within") || inp.contains("Location") || inp.contains("Crime Type") || inp.contains("Context"))){
				
				String [] data = inp.split(",");
				
				if(data.length > 4){
					if((data[4].trim()).length() > 0)
						easternRegionCode = data[4].trim();
					else
						easternRegionCode = "000000";
				}
				else{
					easternRegionCode = "000000";
					northernRegionCode = "000000";
					crimeType = "N.A";
				}
				if(data.length > 5){
					if((data[5].trim()).length() > 0)
						northernRegionCode = data[5].trim();
					else
						northernRegionCode = "000000";
				}
				else{
					northernRegionCode = "000000";
					crimeType = "N.A";
				}
				if(data.length > 7){
					if((data[7].trim()).length() > 0)
						crimeType = data[7].trim();
					else
						crimeType = "N.A";
				}
				
				String key_word = crimeType+","+easternRegionCode+","+northernRegionCode;										
				
				word.set(key_word);
				context.write(word, one);
			}				
		}
		
	}

	public static class CrimeSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration(); 
		Job job = Job.getInstance(conf, "CrimeDatav1");
		
		job.setJarByClass(CrimeDatav1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(CrimeSumReducer.class);
		job.setReducerClass(CrimeSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
