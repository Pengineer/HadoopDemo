package edu.hust.mr.topkey;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 从MapReduce输出的内容中找到value最大的key
 *
 */

public class TopKeyForOne extends Configured implements Tool {

	public static class TopKeyMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		private Text mapOutputKey = new Text();
		private LongWritable mapOutputValue = new LongWritable();
		
		long topKValue = Long.MIN_VALUE;
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			long tempValue = Long.parseLong(line[1]);
			if (tempValue > topKValue) {
				topKValue = tempValue;
				mapOutputKey.set(line[0]);
			}
		}
		
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			mapOutputValue.set(topKValue);
			context.write(mapOutputKey, mapOutputValue);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, TopKeyForOne.class.getName());
		
		job.setJarByClass(TopKeyForOne.class);
		
		//input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(TopKeyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(0);
		
		//output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//commit
		boolean isSuccess = job.waitForCompletion(true);
				
		return isSuccess ? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:9000/user/hadoop/JavaTestout/part-r-00000",
				"hdfs://master:9000/user/hadoop/JavaTestout/out"
		};
		
		int status = new TopKeyForOne().run(args);
		
		System.exit(status);
	}
}
