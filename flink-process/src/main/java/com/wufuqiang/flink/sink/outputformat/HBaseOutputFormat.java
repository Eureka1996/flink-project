package com.wufuqiang.flink.sink.outputformat;

import com.wufuqiang.flink.common.Constants;
import com.wufuqiang.flink.utils.HBaseUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Tuple3<String,String,String>:rowkey,qualify,value
 */
public class HBaseOutputFormat implements OutputFormat<Tuple3<String,String,String>> {

    private HBaseUtils hbaseUtils = null;
    private String tablename = "";
    private String columnFamily = "";



    public HBaseOutputFormat(String tablename){
        this.tablename = tablename;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        hbaseUtils = HBaseUtils.getInstance();
        columnFamily = "data";
    }

    @Override
    public void writeRecord(Tuple3<String,String,String> record) throws IOException {

        hbaseUtils.addRecord(this.tablename,record.f0,columnFamily,record.f1,record.f2);

    }

    @Override
    public void close() throws IOException {

    }
}
