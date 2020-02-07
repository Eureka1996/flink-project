package com.wufuqiang.flink.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class ReadMultiDirectory {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration",true);

        DataSet<String> rawDs = env.readTextFile("/Users/wufuqiang/IdeaProjects/flink-project/flink-process/src/main/resources/source").withParameters(configuration);
        rawDs.print();

        try {
            env.execute("ReadMultiDirectory");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
