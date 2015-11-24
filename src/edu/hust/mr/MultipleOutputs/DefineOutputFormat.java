package edu.hust.mr.MultipleOutputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 自定义数据输出格式
 * 
 * 由于使用的是1.2.1版本，导致MultipleTextOutputFormat继承的FileOutputFormat还是最原始版本的，不是新版
 * 的FileOutputFormat，因此此方式有问题。
 * 
 * 以后更换版本再测试本类。
 * 但是在海量数据情况下，该方案的效率是比较低的，因为它要在map之前先扫描一遍数据，根据数据来判断文件名。
 *
 */
public class DefineOutputFormat {
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}
	
	public static class SaveByCountryOutputFormat extends MultipleTextOutputFormat<NullWritable, Text> {
		@Override
		protected String generateFileNameForKeyValue(NullWritable key, Text value, String fileName) {
			String[] dataRecord = value.toString().split(",", -1);
			String country = dataRecord[1];
			
			return country;
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, DefineOutputFormat.class.getSimpleName());
		job.setJarByClass(DefineOutputFormat.class);
		
		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(SaveByCountryOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
		
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1; 
	}
	
	public static void main(String[] args) throws Exception {
		
		
	}
}
