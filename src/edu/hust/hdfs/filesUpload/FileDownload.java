package edu.hust.hdfs.filesUpload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 下载和移动文件
 * @author hadoop
 *
 */
public class FileDownload {

	public static void main(String[] args) throws IOException {
//		Path dst = new Path("E:\\test\\1.png");
		Path dst = new Path("E:\\test");
		Path src = new Path("hdfs://master:9000/user/hadoop/JavaTest/Image.png");
		
		DownLoadFile(false, src, dst);
		
		System.out.println("over");
	}
	
	public static boolean DownLoadFile(boolean delSrc, Path src, Path dst) throws IOException {
		FileSystem hdfs = getHDFSFileSystem();
		hdfs.copyToLocalFile(delSrc, src, dst);
		
		return true;
	}
	
	 public static void moveToLocalFile(Path src, Path dst) throws IOException {
		 DownLoadFile(true, src, dst);
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
	public static FileSystem getLocalFileSystem() throws IOException {
		Configuration conf = new Configuration();
		return FileSystem.getLocal(conf);
	}
}
