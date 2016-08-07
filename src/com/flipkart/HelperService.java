package com.flipkart;
import java.io.*;
import java.net.*;
import java.util.Map;

import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;



/**
 * Created by surya.kumar on 15/07/16.
 */
public class HelperService {
    public  String getData(String urlToRead) throws Exception {
        StringBuilder result = new StringBuilder();
        try {
            URL url = new URL(urlToRead);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            rd.close();

        }
        catch(Exception ex2){
            ex2.getStackTrace();
        }
        return result.toString();
    }

    public  String writeData(Map<String, JSONObject> map)  {
        Jedis jedis = new Jedis("localhost");
        Pipeline p = jedis.pipelined();
        for(Map.Entry<String, JSONObject> entry : map.entrySet())
            p.set(entry.getKey(), entry.getValue().toString());

        p.sync();
        return "Done writing to redis";
    }

    public  void flushall()  {
        Jedis jedis = new Jedis("localhost");
        jedis.flushAll();

    }



    public static String PostData(String urlToRead,String id) throws Exception {
        String output="";
        try {

            URL url = new URL(urlToRead);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("username", "surya.kumar");
            conn.setRequestProperty("password", "surya@123");

            String input = "{\"query_name\": \"es_shipment_to_order_item\",\"parameters\": {\"VAR_SHIPMENT_ID\": \"" + id + "\"}}";
            OutputStream os = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }
//System.out.println(new BufferedReader(new InputStreamReader((conn.getInputStream()))));
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));
            output = br.readLine();


            conn.disconnect();
        }
        catch (Exception ex2){
            ex2.getStackTrace();
        }
        return output;
    }





}
