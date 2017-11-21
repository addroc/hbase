package com.addroc.hbase1;

import org.apache.hadoop.hbase.TableName;
import org.junit.Test;

public class HBaseToolTest {
	
	private static HBaseTool ht= new HBaseTool();
	//创建表
	/*@Test
	public void testCreateTable() throws Exception{
		ht.createTable("ghgj","teacher","student");
	}*/
	//显示已经创建的表
	/*@Test
	public void showCreatedTable() throws Exception{
		TableName[] table = ht.showCreatedTable();
		for(int i=0;i<table.length;i++){
			System.out.println(table[i]);
		}
	}*/
	
	/*@Test
	public void addData() throws Exception{
		String [] column1=new String[]{
				"name","age","sex"};
		String [] value1=new String[]{
				"zhangzhigang","27","man"
		};
		
		String [] column2=new String[]{
				"name","age"
		};
		String [] value2=new String[]{
				"penttao","16"
		};
		ht.insert("u002", "ghgj", column1, value1, column2, value2);
	}*/
	
	/*@Test
	public void select() throws Exception{
		ht.select("u001", "ghgj");
	}*/
	
	@Test
	public void scan() throws Exception{
		
		ht.scan("u002", "ghgj", "student", "name");
	}
	
	
	
}
