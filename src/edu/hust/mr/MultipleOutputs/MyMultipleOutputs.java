package edu.hust.mr.MultipleOutputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 多文件/文件夹输出
 * 
 * 建议不要在map之前定义数据输入格式来事先扫描一遍数据，然后根据数据来定义输出文件名，这样影响效率，特别是在海量数据情况下，
 * 相当于要扫描两遍源数据。
 * 
 */
public class MyMultipleOutputs {
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
		//将结果输出到多个文件或文件夹
		private MultipleOutputs<NullWritable,Text> mos;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] dataRecord = value.toString().split(",", -1);
			String country = dataRecord[2];
			if (country.equals("asia") || country.equals("la") || country.equals("europe")) {
				mos.write(country,NullWritable.get(), value);
			} else {
				mos.write("otherCountries", key, value);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, MyMultipleOutputs.class.getSimpleName());
		job.setJarByClass(MyMultipleOutputs.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		//配置输出文件名
		MultipleOutputs.addNamedOutput(job, "asia", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "la", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "europe", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "otherCountries", TextOutputFormat.class, NullWritable.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1; 
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:9000/user/hadoop/JavaTest/country.txt",
				"hdfs://master:9000/user/hadoop/JavaTestOut/"
		};
		MyMultipleOutputs MyMultipleOutputs = new MyMultipleOutputs();
		MyMultipleOutputs.removeDir(args[1]);
		
		int status = MyMultipleOutputs.run(args);
		System.exit(status);
		
	}
	
	public void removeDir(String filePath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(filePath), true);
	}
}
