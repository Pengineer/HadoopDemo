package edu.hust.mr.topkey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义数据类型
 */
public class TopKeyWritable implements WritableComparable<TopKeyWritable>{
	String songName;
	String singerName;
	Long playTimes;
	
	public TopKeyWritable(){}
	
	public TopKeyWritable(String songName, String singerName, Long playTimes) {
		set(songName, singerName, playTimes);
	}
	
	public void set(String songName, String singerName, Long playTimes) {
		this.songName = songName;
		this.singerName = singerName;
		this.playTimes = playTimes;
	}

	public String getSongName() {
		return songName;
	}

	public String getSingerName() {
		return singerName;
	}

	public Long getPlayTimes() {
		return playTimes;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(songName);
		out.writeUTF(singerName);
		out.writeLong(playTimes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.songName = in.readUTF();
		this.singerName = in.readUTF();
		this.playTimes = in.readLong();
	}

	@Override
	public int compareTo(TopKeyWritable o) {
		return -(this.getPlayTimes().compareTo(o.getPlayTimes()));
	}
	
	@Override
	public int hashCode() {
		final int prime =31;
		int result =1;
		result = prime * result + singerName.hashCode();
		result = prime * result + songName.hashCode();
		result = prime * result + playTimes.hashCode();
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		TopKeyWritable topKeyWritable  = (TopKeyWritable)obj;
		if (topKeyWritable.getSongName().equals(this.getSongName()) &&
			topKeyWritable.getSingerName().equals(this.getSingerName()) && 
			topKeyWritable.getPlayTimes().equals(this.getPlayTimes())) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return songName + "\t" + singerName + "\t" + playTimes;
	}
}
