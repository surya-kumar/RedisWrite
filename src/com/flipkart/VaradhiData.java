package com.flipkart;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by surya.kumar on 15/07/16.
 */
public class VaradhiData {
    public static String WriteDataToRedis() throws Exception  {
        try {
            HelperService hs = new HelperService();
            String url = "http://varadhi-admin.nm.flipkart.com/queues/vendor_marketplace_production/messages?sidelined=true&offset=0&limit=100000";
            String Data = hs.GetData(url);
            hs.flushall();

            JSONArray newJArray = new JSONArray(Data);
            for (int i=0;i<newJArray.length();i++) {

                String s = newJArray.getString(i);
                JSONObject reader = new JSONObject(s);
                String message_id = reader.getString("message_id");
                String http_response_code = reader.getString("http_response_code");
                String Message = reader.getString("message");
                JSONObject MessageReader = new JSONObject(Message);
                String shipments = MessageReader.getString("shipments");
                JSONArray shipmentArray = new JSONArray(shipments);
                for (int j = 0; j < shipmentArray.length(); j++) {
                    String shipment_param = shipmentArray.getString(j);
                    JSONObject shipmentReader = new JSONObject(shipment_param);
                    String origin_pincode = shipmentReader.getString("origin_pincode");
                    String destination_pincode = shipmentReader.getString("destination_pincode");
                    String merchant_reference_id = shipmentReader.getString("merchant_reference_id");
                    String StatusUrl="http://sp-911.nm.flipkart.com:9000/query/execute/";
                    String StatusResponse=hs.PostData(StatusUrl, merchant_reference_id);


                    JSONObject Readerstatus = new JSONObject(StatusResponse);
                    String hit1=Readerstatus.getString("hits");
                    JSONObject Readerhits = new JSONObject(hit1);
                    String hit2=Readerhits.getString("hits");
                    JSONArray hitArray = new JSONArray(hit2);
                    String hit3 = hitArray.getString(0);
                    JSONObject req_fields = new JSONObject(hit3);
                    String fields=req_fields.getString("fields");
                    JSONObject req_fields1 = new JSONObject(fields);
                    String orderOb=req_fields1.getString("order_id");
                    JSONArray orderArray = new JSONArray(orderOb);
                    String order = orderArray.getString(0);

                    String statusOb=req_fields1.getString("units.state");
                    JSONArray statusArray = new JSONArray(statusOb);
                    String order_status = statusArray.getString(0);
                    System.out.println(fields);
                    String seller_id = shipmentReader.getString("seller_id");
                    String size = shipmentReader.getString("size");
                    String hand_to_hand_pickup = shipmentReader.getString("hand_to_hand_pickup");
                    String amount_to_collect = shipmentReader.getString("amount_to_collect");

                    Map<String, String> RedisMap = new HashMap<>();
                    RedisMap.put("message_id", message_id);
                    RedisMap.put("http_response_code", http_response_code);
                    RedisMap.put("origin_pincode", origin_pincode);
                    RedisMap.put("destination_pincode", destination_pincode);
                    RedisMap.put("seller_id", seller_id);
                    RedisMap.put("size", size);
                    RedisMap.put("hand_to_hand_pickup", hand_to_hand_pickup);
                    RedisMap.put("amount_to_collect", amount_to_collect);
                    RedisMap.put("order", order);
                    RedisMap.put("order_status", order_status);


                    String writeResponse1=hs.WriteData(merchant_reference_id,RedisMap);


                }
            }


//            System.out.println(Data);

        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return "success";
    }

}
