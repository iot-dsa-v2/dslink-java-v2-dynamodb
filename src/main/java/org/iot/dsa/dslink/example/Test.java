package org.iot.dsa.dslink.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

import org.iot.dsa.dslink.dynamodb.Util;
import org.iot.dsa.logging.DSLogger;
import org.iot.dsa.node.DSMap;

import java.util.List;

public class Test extends DSLogger {

    public static void main(String[] args){
        AmazonDynamoDB client = Util.connectDynamoDB("Jana@1990",
                "Enator@1990",
                "ap-south-1","http://localhost:8000");

        //donnectDynamodb();
        //testScan(client);
        //deleteItem(client);
        //testPut(client);
        //testScan(client);
        //deleteItem(client);
        //updateItem123(client);
        //testJSON();
        //testQuery(client);
        Test test = new Test();
        test.getRegions();

    }

    private void testBatchWrite(AmazonDynamoDB client){
        String jsonArray = "[\n" +
                "{ \n" +
                "        \"Artist\": \"Ketan\",\n" +
                "        \"SongTitle\" : \"Sample Song 6\",\n" +
                "\t\t\"year\" : 2019\n" +
                "    },\n" +
                "\t{ \n" +
                "        \"Artist\": \"Ketan\",\n" +
                "        \"SongTitle\" : \"Sample Song 7\",\n" +
                "\t\t\"year\" : 2020\n" +
                "    },\n" +
                "]";

        Util.batchPutItems(new DynamoDB(client),"MusicCollection",jsonArray);
    }

    private void testPut(AmazonDynamoDB client){
        String itemStr = "{\n" +
                "        \"year\": 2018,\n" +
                "        \"title\": \"movie2018\",\n" +
                "        \"info\": {\n" +
                "            \"directors\": [\"SomeOne\"],\n" +
                "            \"release_date\": \"2018-09-02T00:00:00Z\",\n" +
                "            \"rating\": 9.5,\n" +
                "            \"genres\": [\n" +
                "                \"Action\",\n" +
                "                \"Comedy\",\n" +
                "            ],\n" +
                "            \"image_url\": \"http://ia.media-imdb.com/images/M/MV5BMTQyMDE0MTY0OV5BMl5BanBnXkFtZTcwMjI2OTI0OQ@@._V1_SX400_.jpg\",\n" +
                "            \"plot\": \"Some data which is not important.\",\n" +
                "            \"rank\": 5,\n" +
                "            \"running_time_secs\": 8904,\n" +
                "            \"actors\": [\n" +
                "                \"actor1\",\n" +
                "                \"actor2\",\n" +
                "                \"actor3\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }";
        /*String itemStr = "{ \n" +
                "        \"Artist\": \"Ketan\",\n" +
                "        \"SongTitle\" : \"Sample Song\"\n" +
                "    }";*/
        //String itemStr = "{\"EmpId\":1007,\"EmpName\":\"G\",\"Designation\":\"CEO\",\"Salary\":\"98000\"}";

        Util.putItem(new DynamoDB(client),
                "Employee",
                itemStr,
                "",
                "",
                "",
                "TOTAL",
                "SIZE",
                "ALL_OLD");

    }

    private void testQuery123456(AmazonDynamoDB client){
        DSMap result = Util.queryDynamodb(new DynamoDB(client),
                //"MusicCollection",
                "Movies",
                "",
                //"",
                //"Artist = :artName ",
                "#yr=:yrval",
                "",
                "{\"#yr\":\"year\"}",
                //"{\":artName\": {\"S\": \"Ketan\"},\":title1\": {\"S\": \"Sample Song\"}}",
                //"{\":artName\": {\"S\": \"Ketan\"}}",
                "{\":yrval\":{\"N\":\"2006\"}}",
                //"{\"sensortype\":{\"S\":\"currentVoltage\"},\"sensordatetime\":{\"S\":\"2018-11-29T15:38:16:215\"}}",
                "{\"year\":{\"N\":\"2006\"},\"title\":{\"S\":\"All the King's Men\"}}",
                "",
                10,
                true,
                false,
                "TOTAL");

        info(result);
    }

//    private void testQuery(AmazonDynamoDB client){
//        listTable(client);
//        String result = Util.queryDynamodb(new DynamoDB(client),
//                "Movies",
//                "",
//                "#yr = :yyyy",
//                "",
//                "{\"#yr\": \"year\"}",
//                "{\":yyyy\": {\"N\": \"1985\"}}",
//                true,
//                false,
//                "NONE");
//
//        System.out.println(result);
//    }

    private void testQuery(AmazonDynamoDB client){
        DSMap result = Util.queryDynamodb(new DynamoDB(client),
                "Movies",
                "#yr",
                "#yr=:yrval",
                "",
                "{\"#yr\":\"year\"}",
                "{\":yrval\":{\"N\":\"2000\"}}",
                "",
                "",
                0,
                true,
                false,
                "TOTAL");

        info(result);
    }

    private void testScan(AmazonDynamoDB client){
        String result = Util.scanDynamodb(new DynamoDB(client),
                "SimpleCatlog",
                "",
                0,
                "Id=:val",
                "",
                "{\":val\":{\"N\":\"202\"}}",
                "ALL_ATTRIBUTES",
                false,
                0,
                0,
                "",
                "NONE");

        info(result);
    }

    private void deleteItem(AmazonDynamoDB client) {
        Util.deleteItem(new DynamoDB(client),
                "Employee",
                "{\"EmpId\":{\"N\":\"20001\"},\"Salary\":{\"S\":\"56000\"}}",
                "",
                "",
                "",
                "",
                "",
                "NONE");
    }

    private void updateItem(AmazonDynamoDB client) {
        Util.updateItem(new DynamoDB(client),
                "Employee",
                "{\"EmpId\":{\"N\":\"1006\"},\"Salary\":{\"S\":\"9000\"}}",
                "SET EmpName = :name",
                "",
                "",
                "{\":name\":{\"S\":\"Rick\"}}",
                "",
                "",
                "ALL_NEW");
    }

    private void updateItem12345(AmazonDynamoDB client) {
        Util.updateItem(new DynamoDB(client),
                "Employee",
                "{\"EmpId\":{\"N\":\"1006\"},\"Salary\":{\"S\":\"9000\"}}",
                "SET EmpName :name",
                "",
                "",
                "{\":q\":{\"N\":\"666\"}}",
                "",
                "",
                "ALL_NEW");
    }


    private void updateItem123(AmazonDynamoDB client) {
        Util.updateItem(new DynamoDB(client),
                "SimpleCatlog",
                "{\"Id\":{\"N\":\"202\"},\"Title\":{\"S\":\"21-Bike-202\"}}",
                "DELETE Color :p",
                "",
                "",
                "{\":p\":{\"SS\":[\"Blue\",\"Yellow\"]}}",
                "",
                "",
                "NONE");
    }

    private void testEmoScan(AmazonDynamoDB client){
        String result = Util.scanDynamodb(new DynamoDB(client),
                "SimpleCatlog",
                "",
                3,
                "",
                "",
                "",
                "",
                false,
                0,
                0,
                "{\"EmpId\":{\"N\":\"1001\"},\"Salary\":{\"S\":\"100000\"}}",
                "INDEXES");

        System.out.println(result);
    }

    private void testJSON(){
        //String jsonStr = "{ \":v1\": {\"S\": \"No One You Know\"} }";
        //String jsonStr = "{ \":v1\": [\"S\",\"No One You Know\"] }";
        String jsonStr = "{\n" +
                "\t\":B\": {\"B\": \"dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk\"},\n" +
                "\t\":BOOL\": {\"BOOL\": true},\n" +
                "\t\":BS\": {\"BS\":[\"U3Vubnk=\", \"UmFpbnk=\", \"U25vd3k=\"]},\n" +
                "\t\":L\": {\"L\": [\"Cookies\", \"Coffee\", 3.14159]},\n" +
                "\t\":M\": {\"M\": {\"Name\": {\"S\": \"Joe\"}, \"Age\": {\"N\": \"35\"}}},\n" +
                "\t\":N\": {\"N\": \"123.45\"},\n" +
                "\t\":NS\": {\"NS\": [\"42.2\", \"-19\", \"7.5\", \"3.14\"]},\n" +
                "\t\":NULL\": {\"NULL\": true},\n" +
                "\t\":S\": {\"S\": \"Hello\"},\n" +
                "\t\":SS\": {\"SS\": [\"Giraffe\", \"Hippo\" ,\"Zebra\"]}\n" +
                "}";
        DSMap map = Util.parseJsonMap(jsonStr);
        for (DSMap.Entry en : map) {
            info(en.getKey() + " " + en.getValue() + " " + ((DSMap)en.getValue()).firstEntry().getValue().getClass() );
        }
        Util.getValueMap(map);
    }

    private void donnectDynamodb(){

        //AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
        //        .withRegion(Regions.US_WEST_2)
        //        .build();

        //AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
        //        new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
        //        .build();

        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAJFEF2JMGSFLRFDYQ",
                "l1lI3g9Jve8ir/RImI7c7JuHcwSY7jeytqmzJmyc");
        AmazonDynamoDBClientBuilder dynamoDBclientBuilder = AmazonDynamoDBClientBuilder.standard();
        dynamoDBclientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCreds));
        //dynamoDBclientBuilder.setRegion(Region.getRegion(Regions.US_WEST_2));
        dynamoDBclientBuilder.setRegion("us-west-2");
        AmazonDynamoDB client = dynamoDBclientBuilder.build() ;
        //AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build() ;
        client.listTables();

    }

    private void listTable(AmazonDynamoDB dynamoDB) {
        ListTablesResult tables = dynamoDB.listTables();
        List tnames = tables.getTableNames();
        for(int i = 0 ; i < tnames.size();i++){
            info(tnames.get(i));
        }
    }


    private void getRegions() {
        for (Regions region : Regions.values()) {
            info("region :"+ region);
        }
    }
}

