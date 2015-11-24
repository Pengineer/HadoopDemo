package edu.hust.hdfs.filesUpload.auxiliary;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 自定义 路径过滤器
 *
 */
public class MyPathFilter implements PathFilter {
	
	private final String regex;
	
	public MyPathFilter(String regex) {
		this.regex = regex;
	}

	@Override
	public boolean accept(Path path) {
		return path.toString().matches(regex);
	}

}
