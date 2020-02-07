package com.wufuqiang.itemcf.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class MyJedisCluster {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static volatile MyJedisCluster myJedisCluster = null;

    private JedisCluster jedisCluster = null;

    private MyJedisCluster(String redisNodes){
        String redisClusterNodes = redisNodes;
        System.out.println("redisClusterNodes:"+redisClusterNodes);
        String[] hosts = redisClusterNodes.split(",");
        Set<HostAndPort> nodes = new LinkedHashSet<>();
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(1);
        for(String host : hosts){
            String[] hostSplited = host.split(":");
            nodes.add(new HostAndPort(hostSplited[0],Integer.valueOf(hostSplited[1])));
        }
        jedisCluster = new JedisCluster(nodes,1000,10);
    }


    public static MyJedisCluster getMyJedisCluster(String redisNodes){
        if(myJedisCluster == null){
            synchronized(MyJedisCluster.class){
                if(myJedisCluster == null){
                    myJedisCluster = new MyJedisCluster(redisNodes);
                }
            }
        }
        return myJedisCluster ;
    }

    public void pubToRedis(String key,String[] values){

        if(StringUtils.isNotBlank(key) && values != null) {
            try{
                jedisCluster.del(key);
                jedisCluster.rpush(key, values);
                jedisCluster.expire(key, 7 * 24 * 60 * 60);
            }catch(Exception e ){
                e.printStackTrace();
                logger.error("redis key:"+key);
            }
        }

    }


    public String get(String key) throws ExecutionException, InterruptedException {
        String s = jedisCluster.get(key);
        return s ;
    }




    //关闭JedisCluster
    public void closeJediCluster(){

    }
}