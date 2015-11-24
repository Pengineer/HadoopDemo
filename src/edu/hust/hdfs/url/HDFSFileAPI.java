package edu.hust.hdfs.url;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 通过URL的方式操作HDFS
 *
 */
public class HDFSFileAPI {
	
	//静态代码块，初始化一次：让Java识别HDFS的URL
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) {
		URLAccessHDFS();
	}
	
	//Run As Java
	public static void URLAccessHDFS() {
		InputStream in = null;
		String fileUrl = "hdfs://master:9000/user/hadoop/JavaTest/createFile.txt";
		
		try {
			in = new URL(fileUrl).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
