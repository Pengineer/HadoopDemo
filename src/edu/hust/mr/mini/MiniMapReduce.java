package edu.hust.mr.mini;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *	最小的MapReduce框架
 *
 *	通过跟踪new Job()，可以发现在JobContext.class类中有所有的默认设置
 */
public class MiniMapReduce {

	/*
	 * Mapper Class
	 */
	
	/*
	 * Reducer Class
	 */
	
	/*
	 * Driver Method
	 * 所有的设置都来自默认设置
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, MiniMapReduce.class.getSimpleName());
		
		job.setJarByClass(MiniMapReduce.class);
		
		/****************************************************/
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
//		job.setCombinerClass(null); 从1.2.1源码来看，CombinerClass如果不设置就为null,但是如果要设置就不能设置null，否者抛异常，真是奇怪。
		job.setPartitionerClass(HashPartitioner.class);
		
		job.setSortComparatorClass(LongWritable.Comparator.class);
		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1; 
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:9000/user/hadoop/JavaTest/teltraffic.txt",
				"hdfs://master:9000/user/hadoop/JavaTestOut"
		};
		
		int status = new MiniMapReduce().run(args);
		
		System.exit(status);
	}
}
