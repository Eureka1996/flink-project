package com.wufuqiang.flink.source.readsnappy;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;

public class UncodeSnappyFile {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> rawDs = env.readFile(new HdfsSnappyFileInputFormat(new Path("filepath")), "filepath");

        try {
            env.execute("UncodeSnappyFile");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
