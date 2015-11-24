package edu.hust.mr.telTraffic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义数据类型：该数据类型作为value的输出，包含四个字段：上行/下行数据包，上行/下行数据流量。
 * 
 * 继承Writable，是为了实现数据的序列化和反序列化，需要实现读写方法，该读写方法并不会被用户调用，这是给MapReduce框架调用的（序列化/反序列化）。
 * 
 * 因为MyDataWriter是作为value输出，不需要比较排序，因此只继承Writable接口，而不继承WritableComparable接口。
 *
 */
public class MyDataWriterAdv implements WritableComparable<MyDataWriterAdv> {

	private int upPackNum;
	private int upPayload;
	private int downPackNum;
	private int downPayload;
	
	public MyDataWriterAdv(){}
	
	public MyDataWriterAdv(int upPackNum, int upPayload, int downPackNum, int downPayload){
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

	/**
	 * 自定义MyDataWriter的输出格式
	 */
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
		
		if (obj instanceof MyDataWriterAdv) {
			MyDataWriterAdv myDataWriter = (MyDataWriterAdv)obj;
			
			return this.upPackNum == myDataWriter.getUpPackNum() &&
					   this.upPayload == myDataWriter.getUpPayload() &&
					   this.downPackNum == myDataWriter.getDownPackNum() &&
					   this.downPayload == myDataWriter.getDownPayload();
		}
		
		return false;
	}

	@Override
	public int compareTo(MyDataWriterAdv obj) {
		MyDataWriterAdv myDataWriter = (MyDataWriterAdv)obj;
		
		int cmp1 = this.upPackNum < myDataWriter.getUpPackNum() ? -1 : (this.upPackNum == myDataWriter.getUpPackNum() ? 0 : 1);
		
		if (cmp1 != 0) {
			return cmp1;
		}
		
		int cmp2 = this.upPayload < myDataWriter.getUpPayload() ? -1 : (this.upPayload == myDataWriter.getUpPayload() ? 0 : 1);
		if (cmp2 != 0) {
			return cmp2;
		}
		
		int cmp3 = this.downPackNum < myDataWriter.getDownPackNum() ? -1 : (this.downPackNum == myDataWriter.getDownPackNum() ? 0 : 1);
		if (cmp3 != 0) {
			return cmp3;
		}
		
		return this.downPayload < myDataWriter.getDownPayload() ? -1 : (this.downPayload == myDataWriter.getDownPayload() ? 0 : 1);
		
/*		错误的写法：
 * 		String first = "" + this.upPackNum + this.upPayload + this.downPackNum + this.downPayload;
		String second = "" + this.getUpPackNum() + this.getUpPayload() + this.getDownPackNum() + this.getDownPayload();
		
		return first.compareTo(second);
		
		比如 :first：    32 3  20 10
			second： 32 10 20 10
		显然应该下面的应该比上面的大，但是转换成字符串后上面的大于下面的。
*/
	}
	
	/** A Comparator optimized for MyDataWriter.
	 *  用于字节流的比较：排序比较不需要将字节流反序列化成对象在比较，直接在字节流中比较。Hadoop官方API推荐用户自定义数据类型要继承该
	 *  WritableComparator就是字节比较基类，它实现了RawComparator接口
	 *  
	 *  对于多个int属性的对象的比较器还不知道怎么创建字节比较器
	 */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(MyDataWriterAdv.class);
		}
		
		/**
		 * b:MyDataWriter对象序列化后的字节数组
		 * s:字节数组的启示位置
		 * l:字节数组的总长度
		 */
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return super.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MyDataWriterAdv m1 = (MyDataWriterAdv)a;
			MyDataWriterAdv m2 = (MyDataWriterAdv)b;
			if (m1.getUpPackNum() != m2.getUpPackNum()) {
				return m1.getUpPackNum() > m2.getUpPackNum() ? 1 : -1;
			} else if (m1.getUpPayload() != m2.getUpPayload()) {
				return m1.getUpPayload() > m2.getUpPayload() ? 1 : -1;
			} else if (m1.getDownPackNum() != m2.getDownPackNum()) {
				return m1.getDownPackNum() > m2.getDownPackNum() ? 1 : -1;
			} else {
				return m1.getDownPayload() > m2.getDownPayload() ? 1 : (m1.getDownPayload() == m2.getDownPayload() ? 0 : -1);
			}
		}
		
	}

}