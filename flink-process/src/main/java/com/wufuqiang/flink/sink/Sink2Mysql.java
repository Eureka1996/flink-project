package com.wufuqiang.flink.sink;

import com.wufuqiang.flink.entries.MysqlItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Sink2Mysql extends RichSinkFunction<MysqlItem> {
    private Connection connection;
    private PreparedStatement ps;
    private String driver = "com.mysql.jdbc.Driver";
    private String url = "jdbc:mysql://172.16.16.19:3306/recom_free?useSSL=false&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useTimezone=true&serverTimezone=GMT%2B8&allowMultiQueries=true";
    private String username = "paradigm4_ro";
    private String password = "paradigm4_ro";
    private String sql = "insert into item_9033(id,item_id,title,tag,content) values(?,?,?,?,?)";

    public Sink2Mysql() {

    }

    public Sink2Mysql(String sql) {
        this.sql = sql;
    }

    public Sink2Mysql(String driver, String url, String username, String password, String sql) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url,username,password);
        ps = connection.prepareStatement(sql);

    }

    @Override
    public void invoke(MysqlItem value, Context context) throws Exception {
        try{
            ps.setString(1,value.getId());
            ps.setString(2,value.getItemId());
            ps.setString(3,value.getTitle());
            ps.setString(4,value.getTag());
            ps.setString(5,value.getContent());
            ps.executeUpdate();

        }catch(Exception e){
            e.printStackTrace();
        }
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
