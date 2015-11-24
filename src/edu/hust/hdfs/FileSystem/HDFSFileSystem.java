package edu.hust.hdfs.FileSystem;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 与HadoopTest工程下的操作类似，只不过此处不需要Run as hadoop, 而是直接run as java
 * 
 * 因为工程的资源包中添加了两个配置文件
 *
 */
public class HDFSFileSystem {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		boolean isSuccess = fs.delete(new Path("/user/hadoop/JavaTestout/"), true);
		
		System.out.println(isSuccess);
	}
}
