package com.navercorp.pinpoint.collector.dao.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseCRUDTest {

	static Configuration cfg = HBaseConfiguration.create();
    static {

        cfg.set("hbase.zookeeper.quorum", "localhost.localdomain");
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
    }

    /**
     * 创建一张表
     */

    public static void createTable(String tableName) {
        System.out.println("************start create table**********");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(cfg);
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
                hBaseAdmin.disableTable(tableName);
                hBaseAdmin.deleteTable(tableName);
                System.out.println(tableName + " is exist");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);// 代表表的schema
            tableDescriptor.addFamily(new HColumnDescriptor("name")); // 增加列簇
            tableDescriptor.addFamily(new HColumnDescriptor("age"));
            tableDescriptor.addFamily(new HColumnDescriptor("gender"));
            hBaseAdmin.createTable(tableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("*****end create table*************");
    }

    /*
     * 向已经存在的表中添加列 ，需要先disable表
     */
    public static void addMyColumn(String tableName,String columnFamily){
            System.out.println("************start add column ************"); 
            HBaseAdmin hBaseAdmin = null;
          try {
            hBaseAdmin = new HBaseAdmin(cfg);
            hBaseAdmin.disableTable(tableName); 
            HColumnDescriptor hd = new HColumnDescriptor(columnFamily);
            hBaseAdmin.addColumn(tableName,hd);
            
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                hBaseAdmin.enableTable(tableName);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
            System.out.println("************end add Column ************"); 

     }

    /*
     * 插入数据
     */

    public static void insert(String tableName) {
        System.out.println("************start insert ************");
        HTablePool pool = new HTablePool(cfg, 1000);
        // HTable table = (HTable) pool.getTable(tableName);

        Put put = new Put("6".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
        put.addColumn("name".getBytes(), null, "Joey".getBytes());// 本行数据的第一列
        put.addColumn("age".getBytes(), null, "20".getBytes());// 本行数据的第三列
        put.addColumn("gender".getBytes(), null, "male".getBytes());// 本行数据的第三列
        put.addColumn("score".getBytes(), "Math".getBytes(), "90".getBytes());// 本行数据的第四列
        put.addColumn("score".getBytes(), "English".getBytes(), "100".getBytes());// 本行数据的第四列
        put.addColumn("score".getBytes(), "Chinese".getBytes(), "100".getBytes());// 本行数据的第四列   第二个参数对应qualifier
        try {
            pool.getTable(tableName).put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("************end insert************");
    }

    /*
     * 查询所有数据
     */

    public static void queryAll(String tableName) {
        HTablePool pool = new HTablePool(cfg, 1000);
        try {
            ResultScanner rs = pool.getTable(tableName).getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                    + "     qualifier:" + new String(keyValue.getQualifier())
                            + "     值:" + new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 查询单条数据
     */
    public static void querySingle(String tableName) {

        HTablePool pool = new HTablePool(cfg, 1000);
        try {
            Get get = new Get("6".getBytes());// 根据rowkey查询
            Result r = pool.getTable(tableName).get(get);
            System.out.println("rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "    值:" + new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 删除数据
     */
    public static void deleteRow(String tablename, String rowkey) {
        try {
            HTable table = new HTable(cfg, tablename);
            List list = new ArrayList();
            Delete d1 = new Delete(rowkey.getBytes());
            list.add(d1);
            table.delete(list);
            System.out.println("删除行成功!");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*
     * 删除表
     */
    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(cfg);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("table: " + tableName + "删除成功！");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] agrs) {
        try {
            String tableName = "wishTest1";
//            HBaseCRUD.addMyColumn(tableName,"score");
            //HBaseCRUDTest.queryAll(tableName);
            //HBaseCRUDTest.createTable(tableName);
            
            //HBaseCRUDTest.addMyColumn(tableName, "score");
            //HBaseCRUDTest.insert(tableName);
            HBaseCRUDTest.querySingle(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
