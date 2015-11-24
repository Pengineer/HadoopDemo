package edu.hust.mr.telTraffic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 自定义数据类型：该数据类型作为value的输出，包含四个字段：上行/下行数据包，上行/下行数据流量。
 * 
 * 继承Writable，是为了实现数据的序列化和反序列化，需要实现读写方法，该读写方法并不会被用户调用，这是给MapReduce框架调用的（序列化/反序列化）。
 * 
 * 因为MyDataWriter是作为value输出，不需要比较排序，因此只继承Writable接口，而不继承WritableComparable接口。
 *
 */
public class MyDataWriter implements Writable {

	private int upPackNum;
	private int upPayload;
	private int downPackNum;
	private int downPayload;
	
	public MyDataWriter(){}
	
	public MyDataWriter(int upPackNum, int upPayload, int downPackNum, int downPayload){
		set(upPackNum, upPayload, downPackNum, downPayload);
	}
	
	public int getUpPackNum() {
		return upPackNum;
	}

	public int getUpPayload() {
		return upPayload;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public int getDownPayload() {
		return downPayload;
	}

	public void set(int upPackNum, int upPayload, int downPackNum, int downPayload){
		this.upPackNum = upPackNum;
		this.upPayload = upPayload;
		this.downPackNum = downPackNum;
		this.downPayload = downPayload;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(upPackNum);
		out.writeInt(upPayload);
		out.writeInt(downPackNum);
		out.writeInt(downPayload);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readInt();  //注意读的顺序要与写的顺序一致
		this.upPayload = in.readInt();
		this.downPackNum = in.readInt();
		this.downPayload = in.readInt();
	}

	@Override
	public String toString() {
		//返回顺序要与上面的保持严格一致
		return upPackNum + "\t" + upPayload + "\t" + downPackNum + "\t" + downPayload;
	}

	@Override
	public int hashCode() {
		final int prime =31;
		int result =1;
		result = prime * result + upPackNum;
		result = prime * result + upPayload;
		result = prime * result + downPackNum;
		result = prime * result + downPayload;
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		
		if (obj instanceof MyDataWriter) {
			MyDataWriter myDataWriter = (MyDataWriter)obj;
			
			return this.upPackNum == myDataWriter.getUpPackNum() &&
				   this.upPayload == myDataWriter.getUpPayload() &&
				   this.downPackNum == myDataWriter.getDownPackNum() &&
				   this.downPayload == myDataWriter.getDownPayload();
		}
		
		return false;
	}

}