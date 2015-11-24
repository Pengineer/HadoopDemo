package edu.hust.mr.telTraffic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 需求：统计分析log文件中不同手机号码产生的数据流量和数据包的总和
 * 
 * log文件提供的字段：
 * 1）主键id
 * 2）手机号
 * 3）上行流量
 * 4）上行数据包
 * 5）下行流量
 * 6）下行数据包
 * 7）http状态码
 * 
 * 显然输出value有四个字段，因此自定义数据类型MyDataWriter，包含这四个字段。
 * 
 */

public class TrafficSumMR extends Configured implements Tool {
	
	enum counter {
		LINESKIP
	}
	
	//Mapper Class：静态内部类
	static class TrafficSumMapper extends Mapper<LongWritable, Text, Text, MyDataWriter> {
		//定义输出参数
		Text outPutKey = new Text();
		MyDataWriter outputvalue = new MyDataWriter();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String[] lineValue = value.toString().split("\t");
				
				String telphone = lineValue[1];
				int upPackNum = Integer.parseInt(lineValue[2]);
				int upPayload = Integer.parseInt(lineValue[3]);
				int downPackNum = Integer.parseInt(lineValue[4]);
				int downPayload = Integer.parseInt(lineValue[5]);
				
				outPutKey.set(telphone);
				outputvalue.set(upPackNum, upPayload, downPackNum, downPayload);
				
				context.write(outPutKey, outputvalue);
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
			
		}
	}
	
	//Reducer Class：静态内部类
	static class TrafficSumReducer extends Reducer<Text, MyDataWriter, Text, MyDataWriter> {
		//定义输出value
		MyDataWriter outputvalue = new MyDataWriter();
		
		@Override
		public void reduce(Text key, Iterable<MyDataWriter> values, Context context)
				throws IOException, InterruptedException {
			int upPackNum = 0;
			int upPayload = 0;
			int downPackNum = 0;
			int downPayload = 0;
			
			for (MyDataWriter value : values) {
				upPackNum += value.getUpPackNum();
				upPayload += value.getUpPayload();
				downPackNum += value.getDownPackNum();
				downPayload += value.getDownPayload();
			}
			
			outputvalue.set(upPackNum, upPayload, downPackNum, downPayload);
			context.write(key, outputvalue);
		}
		
	}
	
	//Driver method
	@Override
	public int run(String[] args) throws Exception {
		//get conf
		Configuration conf = new Configuration();
		
		//create job
		Job job = new Job(conf, TrafficSumMR.class.getSimpleName());
		
		//set job
		job.setJarByClass(TrafficSumMR.class);
		
		//input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//mapper
		job.setMapperClass(TrafficSumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyDataWriter.class);
		
		//reducer
		job.setReducerClass(TrafficSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyDataWriter.class);
		
		//output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//commit
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	
	//Main Method
	public static void main(String[] args) throws Exception {	
		args = new String[]{
				"hdfs://master:9000/user/hadoop/JavaTest/teltraffic.txt",
				"hdfs://master:9000/user/hadoop/JavaTest/telOut"
		};
		
		int status = new TrafficSumMR().run(args);
		System.exit(status);
	}
}
