package com.wufuqiang.flink.source;

import com.wufuqiang.flink.entries.MysqlItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SourceFromMysql extends RichSourceFunction<MysqlItem> {

    private Connection connection;
    private PreparedStatement ps;
    private String driver = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://172.16.16.19:3306/recom_free?useSSL=false&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useTimezone=true&serverTimezone=GMT%2B8&allowMultiQueries=true";
    private String username = "paradigm4_ro";
    private String password = "paradigm4_ro";
    private String sql = "select id,item_id,title,tag,content  from item_9033 where id <=  1000";

    public SourceFromMysql() {
    }

    public SourceFromMysql(String sql) {
        this.sql = sql;
    }

    public SourceFromMysql(String driver, String url, String username, String password, String sql) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        String driver = "com.mysql.jdbc.Driver";
//        String url = "jdbc:mysql://172.16.16.19:3306/recom_free?useSSL=false&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useTimezone=true&serverTimezone=GMT%2B8&allowMultiQueries=true";
//        String username = "paradigm4_ro";
//        String password = "paradigm4_ro";
        Class.forName(driver);
        connection = DriverManager.getConnection(url,username,password);
//        String sql = "select id,item_id,title,tag,content  from item_9033 where id <=  1000";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {

        try{
            ResultSet resultSet = ps.executeQuery();
            while(resultSet.next()){
                MysqlItem mysqlItem = new MysqlItem(
                        resultSet.getString("id"),
                        resultSet.getString("item_id"),
                        resultSet.getString("title"),
                        resultSet.getString("tag"),
                        resultSet.getString("content"));
                ctx.collect(mysqlItem);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if(ps != null){
            ps.close();
        }
        if(connection != null){
            connection.close();
        }
    }
}
