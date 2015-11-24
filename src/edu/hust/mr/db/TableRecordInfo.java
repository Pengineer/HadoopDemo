package edu.hust.mr.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 封装要操作的数据库字段
 *
 */
public class TableRecordInfo implements Writable, DBWritable {

	private int id;
	private String name;
	
	public TableRecordInfo(){}
	
	public TableRecordInfo(int id, String name) {
		this.set(id, name);
	}
	
	public void set(int id, String name) {
		this.id = id;
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(name);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		name = in.readUTF();
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, id);
		statement.setString(2, name);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		//除非使用DBInputFormat直接从数据库输入数据，否则readFields方法不会被调用
		id = resultSet.getInt(1);
		name = resultSet.getString(2);
	}

	@Override
	public String toString() {
		return new String(this.id + "...." + this.name);
	}
}