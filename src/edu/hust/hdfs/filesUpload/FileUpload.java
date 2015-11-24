package edu.hust.hdfs.filesUpload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 单文件上传
 *
 */
public class FileUpload {

	public static void main(String[] args) throws IOException {
		Path src = new Path("E:\\test\\Image.png");
		Path dst = new Path("hdfs://master:9000/user/hadoop/JavaTest/Image.png");
		
		uploadLocalFile(false, true, src, dst);
		
		System.out.println("over");
	}
	
	public static boolean uploadLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
		FileSystem hdfs = getHDFSFileSystem();
		hdfs.copyFromLocalFile(delSrc, overwrite, src, dst);
		
		return true;
	}
	
	/**
	 * 获取配置文件中<fs.default.name>对应主机的文件系统
	 */
	public static FileSystem getHDFSFileSystem() throws IOException {
		Configuration conf = new Configuration();
		return FileSystem.get(conf);
	}
	
	/**
	 * 获取本地文件系统
	 */
	public static LocalFileSystem getLocalFileSystem() throws IOException {
		Configuration conf = new Configuration();
		return FileSystem.getLocal(conf);
	}
	
}
