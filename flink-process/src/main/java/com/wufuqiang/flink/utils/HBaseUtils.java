package com.wufuqiang.flink.utils;

import com.wufuqiang.flink.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class HBaseUtils {

    private static volatile HBaseUtils hbaseUtils = null;
    private static String columnFamily = "data";
    private volatile Connection connection = null;
    private volatile Configuration config = null;


    private HBaseUtils(){

        config = HBaseConfiguration.create();
        config.set("zookeeper.znode.parent","/hbase-unsecure");
        config.set("hbase.zookeeper.quorum","tencent-recom-hdp51,tencent-recom-hdp52,tencent-recom-hdp53");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("");
        if(connection == null){
            System.exit(1);
        }
    }

    public static HBaseUtils getInstance(){
        if(hbaseUtils == null){
            synchronized(HBaseUtils.class){
                if(hbaseUtils == null){
                    hbaseUtils = new HBaseUtils();
                }
            }
        }
        return hbaseUtils;
    }


    public void addRecord(String tablename,String rowkey,String columnFamily,String column,String value) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }




    public void close(){
        if(connection != null){
            synchronized(HBaseUtils.class){
                if(connection != null){
                    try {
                        connection.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}
