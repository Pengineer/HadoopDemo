package edu.hust.hdfs.filesUpload.auxiliary;

import org.apache.hadoop.util.Progressable;

public class MyProgressable implements Progressable {

	@Override
	public void progress() {
		System.out.print(".");
	}

}
