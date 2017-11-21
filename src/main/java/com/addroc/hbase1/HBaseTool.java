package com.addroc.hbase1;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;

public class HBaseTool {
	private static Configuration conf = HBaseConfiguration.create();
	private static  String zk_list = "hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181";
	static{
		conf.set("hbase.zookeeper.quorum", zk_list);
	}
	
	public static void createTable(String tableName,String...cfs) throws Exception{
		
		//通过连接获取hbase的admin
		try(Connection connection = ConnectionFactory.createConnection(conf);
				Admin admin = connection.getAdmin();){
			
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
			if(cfs!=null){
				for(String cf:cfs){
					htd.addFamily(new HColumnDescriptor(cf.getBytes()));
				}
			}
			System.out.println("create table ...");
			if(admin.tableExists(TableName.valueOf(tableName))){
				System.out.println("create table failure!!");
				System.out.println("the table you want created is exists!!!");
			}else{
				admin.createTable(htd);
				System.out.println("the table you want have been created!!");
			}
			
		}
	}
	
	public static TableName [] showCreatedTable() throws Exception{
		
		try(Connection connection = ConnectionFactory.createConnection(conf);
				Admin admin = connection.getAdmin();){
			return admin.listTableNames();
		}
	}
	
	public static void insert(String rowKey,String tableName,String[] column1,String[] value1,
			String[] column2,String[] value2) throws Exception{
		try(Connection connection = ConnectionFactory.createConnection(conf);
				Admin admin = connection.getAdmin();){
			
			Table table = connection.getTable(TableName.valueOf(tableName));
			Put put=new Put(rowKey.getBytes());
			Collection<HColumnDescriptor> fa = table.getTableDescriptor().getFamilies();
			for(HColumnDescriptor hc:fa){
				String familyname = hc.getNameAsString();
				
				if(familyname.equals("student")){
					for(int i=0;i<column1.length;i++){
						put.addColumn(familyname.getBytes(), column1[i].getBytes(), value1[i].getBytes());
					}
				}
				
				if(familyname.equals("teacher")){
					for(int i=0;i<column2.length;i++){
						put.addColumn(familyname.getBytes(), column2[i].getBytes(), value2[i].getBytes());
					}
				}
			}
			
			table.put(put);
			System.out.println("add data success!!!");
		}
		
	}
	
	
	public static void select(String rowKey,String tableName) throws Exception{
		try(Connection connection = ConnectionFactory.createConnection(conf);
				Admin admin = connection.getAdmin();){
			Table table = connection.getTable(TableName.valueOf(tableName));
			Get get=new Get(rowKey.getBytes());
			Result rs = table.get(get);
			
			/*KeyValue[] raw = rs.raw();
			for(KeyValue ky:raw){
				//System.out.println(ky.toString());
				System.out.println(new String(ky.getFamily(),"utf-8"));
				
			}*/
			Cell[] rawCells = rs.rawCells();
			for(Cell c:rawCells){
				System.out.println(new String(c.getFamily())+":"+new String(c.getQualifier())+"\t"+c.getTimestamp()+","+new String(c.getValue()));
			}
			
		}
	}
	
	
	public static void scan(String rowKey,String tableName,String cf,String column) throws Exception{
		try(Connection connection = ConnectionFactory.createConnection(conf);
				Admin admin = connection.getAdmin();){
			
			Table table = connection.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(rowKey.getBytes()));
			
			scan.setFilter(rowFilter);
			ResultScanner rs = table.getScanner(scan);
			Iterator<Result> iterator = rs.iterator();
			while(iterator.hasNext()){
				Result next = iterator.next();
				Cell cell = next.getColumnCells(cf.getBytes(), column.getBytes()).get(0);
				System.out.println(new String(CellUtil.cloneValue(cell)));
			}
			
		}
	}
}

