package com.flipkart;

import redis.clients.jedis.Jedis;

/**
 * Created by surya.kumar on 15/07/16.
 */
public class RedisWriteService {
    public static void main(String[] args) throws Exception{
        //Connecting to Redis server on localhost
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server sucessfully");
        //check whether server is running or not
        System.out.println("Server is running: "+jedis.ping());
        VaradhiData vd = new VaradhiData();
        String res=vd.WriteDataToRedis();
        System.out.println("response"+res);


    }
}
