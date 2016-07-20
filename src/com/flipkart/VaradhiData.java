package com.flipkart;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by surya.kumar on 15/07/16.
 */
public class VaradhiData {

    private final ExecutorService threadPool = Executors.newFixedThreadPool(20);
    public  String WriteDataToRedis() throws Exception  {
        try {
            final HelperService hs = new HelperService();
            String url = "http://varadhi-admin.nm.flipkart.com/queues/vendor_marketplace_production/messages?sidelined=true&offset=0&limit=100000";
            String Data = hs.getData(url);
            hs.flushall();
            final String StatusUrl = "http://sp-911.nm.flipkart.com:9000/query/execute/";
            JSONArray newJArray = new JSONArray(Data);
            Map<String, JSONObject> requestMap = new HashMap<>();
            Map<String, Future<String>> responseMap = new HashMap<>();
            for (int i=0;i<newJArray.length();i++) {

                String s = newJArray.getString(i);
                JSONObject reader = new JSONObject(s);
                String message_id = reader.getString("message_id");
                final String merchantReferenceId = reader.getString("group_id");
                String http_response_code = reader.getString("http_response_code");
                String Message = reader.getString("message");
                JSONObject MessageReader = new JSONObject(Message);
                String shipments = MessageReader.getString("shipments");
                JSONArray shipmentArray = new JSONArray(shipments);
                requestMap.put(merchantReferenceId, reader);

                for (int j = 0; j < shipmentArray.length(); j++) {
                    String shipment_param = shipmentArray.getString(j);
                    JSONObject shipmentReader = new JSONObject(shipment_param);
                    responseMap.put(merchantReferenceId, threadPool.submit(new Callable<String>() {
                        @Override
                        public String call() throws Exception {
                            if (merchantReferenceId.startsWith("R-"))
                            {
                                merchantReferenceId.replace("R-","");
                            }
                            return hs.PostData(StatusUrl, merchantReferenceId);
                        }
                    }));
                }
            }

            Map<String , Map<String, String>> dataMap = new HashMap<>();

            for(Map.Entry<String, JSONObject> entry : requestMap.entrySet()) {
                JSONObject reader = entry.getValue();
                String message_id = reader.getString("message_id");
                final String merchantReferenceId = reader.getString("group_id");
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
                    Future<String> futureResponse = responseMap.get(entry.getKey());
                    String orderId = "NA";
                    String orderStatus = "NA";
                    if(null != futureResponse.get()) {
                        JSONObject Readerstatus = new JSONObject(futureResponse.get());
                        String hit1 = Readerstatus.getString("hits");
                        JSONObject Readerhits = new JSONObject(hit1);
                        String hit2 = Readerhits.getString("hits");
                        JSONArray hitArray = new JSONArray(hit2);
                        if (hitArray.length() != 0) {
                            String hit3 = hitArray.getString(0);
                            JSONObject req_fields = new JSONObject(hit3);
                            String fields = req_fields.getString("fields");
                            JSONObject req_fields1 = new JSONObject(fields);
                            String orderOb = req_fields1.getString("order_id");
                            JSONArray orderArray = new JSONArray(orderOb);
                            orderId = orderArray.getString(0);

                            String statusOb = req_fields1.getString("units.state");
                            JSONArray statusArray = new JSONArray(statusOb);
                            orderStatus = statusArray.getString(0);
                        }
                    }
                    //  System.out.println(fields);
                    String seller_id = shipmentReader.getString("seller_id");
                    String size = shipmentReader.getString("size");
                    String hand_to_hand_pickup = shipmentReader.getString("hand_to_hand_pickup");
                    String amount_to_collect = shipmentReader.getString("amount_to_collect");

                    Map<String, String> redisMap = new HashMap<>();
                    redisMap.put("message_id", message_id);
                    redisMap.put("http_response_code", http_response_code);
                    redisMap.put("origin_pincode", origin_pincode);
                    redisMap.put("destination_pincode", destination_pincode);
                    redisMap.put("seller_id", seller_id);
                    redisMap.put("size", size);
                    redisMap.put("hand_to_hand_pickup", hand_to_hand_pickup);
                    redisMap.put("amount_to_collect", amount_to_collect);
                    redisMap.put("order", orderId);
                    redisMap.put("order_status", orderStatus);


                    dataMap.put(merchantReferenceId, redisMap);

                }
            }

            String writeResponse1=hs.writeData(dataMap);
            System.out.println(writeResponse1);

        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return "success";
    }

}
