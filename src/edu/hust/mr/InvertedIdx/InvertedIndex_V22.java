package edu.hust.mr.InvertedIdx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


/**
 *由于reduce的输入无法将相同的单词形成一个list集合，因此考虑使用两个MapReduce。
 *
 * 思路2：(w-单词，f-词频，d-文件)
 * job1的Map端：
 * 1，map()：将每个文件按照<w, d>键值对的形式输出；
 * 2，combiner()：将每个文件中相同的单词累加，统计出该文件的单词词频，形成新的键值对<w:f, d>；
 * 3，MapReduce框架会自动将combiner的输出结果按key进行多次排序，<w1:2, d1> <w1:3, d3> <w2:2, d1> <w3:1, d2>....；
 * 4，patition()：将排序后的结果进行分区，使用自定义分区方式（第3步排序时为了方便第4步的快速分区）；
 * Reduce端：
 * 5，MapReduce框架自动将每个分区的结果进行排序汇总，形成<key,list<value>>；
 * 6，reduce()：拆分list<value>，改变组合形式，因为从patition到reduce的过程可能是这样的：
 * 	  w1:2  d1            w1:2  list<d1,d2>              w1  d1:2
 *    w1:2  d2      ==>   w1:3  list<d3>         ==>     w1  d2:2
 *    w1:3  d3                                           w1  d3:3
 *   patition输出                                    reduce的输入                                                       reduce的输出
 * job2的Map端：
 * 7，map():为了保证同一个单词交给同一个reduce，将输入拆分成<w, d:f>键值对的形式直接交给reduce；
 * 8，reduce():组合，生成文档列表
 */
public class InvertedIndex_V22 extends Configured implements Tool {
	
	enum counter {
		LINESKIP
	}
	
	static class InvertedIndexMapper1 extends Mapper<LongWritable, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		FileSplit split;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				split = (FileSplit) context.getInputSplit();
				StringTokenizer strs = new StringTokenizer(value.toString());
				while (strs.hasMoreElements()) {
					String str = (String) strs.nextElement();
					outputKey.set(str);
					outputValue.set(split.getPath().getName());
					context.write(outputKey, outputValue);
				}
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
		}
	}
	
	/**
	 * 统计各文档的词频
	 */
	static class InvertedIndexCombiner1 extends Reducer<Text, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text outputValue = new Text();
			try {
				//<word, list<doc1,doc1,doc1>> 统计word的在单文件中的词频
				int sum =0;
				for (Text value : values) {
					sum++;
					if(value != null){
						outputValue = value;//测试发现values的第一个元素为空
					}
				}
				
				String outputkey = key.toString() + ":" + sum;
				outputKey .set(outputkey);
				context.write(outputKey, outputValue);
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
		}
	}
	
	/**
	 * 按单词分区
	 */
	static class WordPatitioner1 extends HashPartitioner<Text, Text> {
		public int getPartition(Text key, Text value, int numReduceTasks) {
			key = new Text(key.toString().split(":")[0]);
			return super.getPartition(key , value, numReduceTasks);
		};
	}
	
	/**
	 * 改变组合形式
	 */
	static class InvertedIndexReducer1 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] line = key.toString().split(":");
			String word = line[0];
			String freq = line[1];
			
			for (Text t : values) {
				context.write(new Text(word), new Text(t.toString() + ":" + freq));
			}
		}
		
	}
////////////////////////////////////////////////////////////////////////////////////////////
	
/*	job1的输出结果
	Everything	1.txt:1
	Everything	2.txt:2
	MapReduce	2.txt:1
	MapReduce	1.txt:2
	MapReduce	3.txt:3
	hello	3.txt:1
	is	2.txt:1
	is	1.txt:2
	powerful	2.txt:1
	simple	2.txt:1
	simple	1.txt:3
*/

	static class InvertedIndexMapper2 extends Mapper<LongWritable, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//为了保证同一个单词交给同一个reduce，key还是单词，不能直接：context.write(NullWritable.get(), value);
			String[] line = value.toString().split("\t");
			outputKey.set(line[0]);
			outputValue.set(line[1]);
			context.write(outputKey, outputValue);
		}
	}
	
	/**
	 * 生成文档列表
	 */
	static class InvertedIndexReducer2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String doc="";
			for (Text text : values) {
				doc = text.toString() + "; " + doc;//doc += text.toString() + "; ";    控制输出顺序
			}
			context.write(key, new Text(doc));
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(InvertedIndex_V22.class);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: InvertedIndex <in> <out>");
			System.exit(2);
		}
		
		FileSystem hdfs = FileSystem.get(conf);
		Path inPath = new Path(args[0]);
		Path tmpPath = new Path(args[0] + "tmp/");
		Path outPath = new Path(args[1]);
		if (hdfs.exists(tmpPath)) {
			hdfs.delete(tmpPath, true);
		}
		if (hdfs.exists(outPath)) {
			hdfs.delete(outPath, true);
		}
		
		//job1
		Job job1 = new Job(conf, "job1");
		job1.setJarByClass(InvertedIndex_V22.class);
		
		FileInputFormat.addInputPath(job1, inPath);
		FileOutputFormat.setOutputPath(job1, tmpPath);

		job1.setMapperClass(InvertedIndexMapper1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setCombinerClass(InvertedIndexCombiner1.class);
		
		job1.setReducerClass(InvertedIndexReducer1.class);
		job1.setNumReduceTasks(2);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		//job2
		Job job2 = new Job(conf, "job2");
		job2.setJarByClass(InvertedIndex_V22.class);
		
		FileInputFormat.addInputPath(job2, tmpPath);
		FileOutputFormat.setOutputPath(job2, outPath);

		job2.setMapperClass(InvertedIndexMapper2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setReducerClass(InvertedIndexReducer2.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		return handleJobChain(job1, job2, "JobGroup");
	}
	
	/**
	 * 组合多个Job，相互之间可以有依赖
	 * @param job1
	 * @param job2
	 * @param chainName  任务组名
	 * @return
	 * @throws IOException
	 */
	public static int handleJobChain(Job job1 ,Job job2, String chainName) throws IOException{ 
		ControlledJob ctrlJob1 = new ControlledJob(job1.getConfiguration());//为每一个job创建一个控制器
		ctrlJob1.setJob(job1);
		
		ControlledJob ctrlJob2 = new ControlledJob(job2.getConfiguration());
		ctrlJob2.setJob(job2);
		ctrlJob2.addDependingJob(ctrlJob1);//指定job2依赖于job1
		
		JobControl jobCtrl = new JobControl(chainName);//任务组控制器，控制改组的所有任务
		jobCtrl.addJob(ctrlJob1);
		jobCtrl.addJob(ctrlJob2);
		
		Thread t = new Thread(jobCtrl);//hadoop的JobControl类实现了线程Runnable接口。我们需要实例化一个线程来让它启动。
		t.start();
		
		while (true) {
			if (jobCtrl.allFinished()) {
				System.out.println(jobCtrl.getSuccessfulJobList());
				jobCtrl.stop();//终止线程
				return 0;
			}
			
			if (jobCtrl.getFailedJobList().size() > 0) {
				System.out.println(jobCtrl.getFailedJobList());  
				jobCtrl.stop();  
				return 1;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:9000/user/hadoop/InvertedIdx/",
				"hdfs://master:9000/user/hadoop/InvertedIdxOut2/"
		};
		
		int status = new InvertedIndex_V22().run(args);
		System.exit(status);
	}
}
