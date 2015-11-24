package edu.hust.hdfs.filesUpload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import edu.hust.hdfs.filesUpload.auxiliary.MyPathFilter;

/**
 * 将本地的多个小文件合并上传到HDFS
 * 
 */
public class FilesUploadToOne {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Path src = new Path("E:\\test");
		Path dst = new Path("hdfs://master:9000/user/hadoop/JavaTest/merge.data");
		mergeAndUpload(src, dst);
		
		System.out.println("over");
	}
	
	public static boolean mergeAndUpload(Path localDirPath, Path HdfsFilePath) throws IOException {
		return mergeAndUpload(localDirPath, HdfsFilePath, new MyPathFilter(".*txt"));
	}
	
	/**
	 * 合并所有输入流到同一输出流，并上传文件
	 * @param localDirPath  本地要上传的文件目录
	 * @param HdfsFilePath  HDFS上的文件名称
	 * @param filter  文件过滤器
	 * @return
	 * @throws IOException
	 */
	public static boolean mergeAndUpload(Path localDirPath, Path HdfsFilePath, PathFilter filter) throws IOException {
		FileSystem localfs = getLocalFileSystem();
		FileSystem hdfs = getHDFSFileSystem();
		
		FSDataOutputStream outputStream = hdfs.create(HdfsFilePath);
		FSDataInputStream inputStream = null;
		FileStatus[] fileStatus = localfs.listStatus(localDirPath, filter);
		for (FileStatus status : fileStatus) {
			Path path = status.getPath();
			System.out.println(path.getName());
			inputStream = localfs.open(path);
			byte[] buffer = new byte[2048];
			int len=0;
			while ((len = inputStream.read(buffer)) > 0 ) {
				outputStream.write(buffer, 0, len);
			}
			inputStream.close();
		}
		outputStream.close();
		
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
	public static FileSystem getLocalFileSystem() throws IOException {
		Configuration conf = new Configuration();
		return FileSystem.getLocal(conf);
	}

}
