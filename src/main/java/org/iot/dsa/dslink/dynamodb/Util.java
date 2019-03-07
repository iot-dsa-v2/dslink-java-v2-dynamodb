package org.iot.dsa.dslink.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnItemCollectionMetrics;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import org.iot.dsa.io.json.JsonReader;
import org.iot.dsa.logging.DSLogger;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSFlexEnum;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.util.DSException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Util {

    public static AmazonDynamoDB connectDynamoDB(String accesskey,String secretKey, String region,String endPoint){
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accesskey, secretKey);
        AmazonDynamoDBClientBuilder dynamoDBclientBuilder = AmazonDynamoDBClientBuilder.standard();
        dynamoDBclientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCreds));
        if(endPoint==null || endPoint.trim().equals("")) {
            dynamoDBclientBuilder.setRegion(region);
        }else {
            AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(endPoint, region);
            dynamoDBclientBuilder.setEndpointConfiguration(endpoint);
        }
        return dynamoDBclientBuilder.build() ;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Query Items
    ///////////////////////////////////////////////////////////////////////////

    public static DSMap queryDynamodb(DynamoDB client,
                                       String tableName,
                                       String ProjectionExpression,
                                       String KeyConditionExpression,
                                       String FilterExpression,
                                       String ExpressionAttributeNames,
                                       String ExpressionAttributeValues,
                                       String ExclusiveStartKey,
                                       String SelectS,
                                       int maxResultSize,
                                       boolean ScanIndexForward,
                                       boolean ConsistentRead,
                                       String ReturnConsumedCapacitys){

        Table table = client.getTable(tableName);

        QuerySpec querySpec = new QuerySpec();
        if(ProjectionExpression!=null && !ProjectionExpression.trim().equals("")){
            querySpec = querySpec.withProjectionExpression(ProjectionExpression.trim());
        }
        if(KeyConditionExpression!=null && !KeyConditionExpression.trim().equals("")){
            querySpec = querySpec.withKeyConditionExpression(KeyConditionExpression.trim());
        }
        if(FilterExpression!=null && !FilterExpression.trim().equals("")){
            querySpec = querySpec.withFilterExpression(FilterExpression.trim()) ;
        }
        if(ExpressionAttributeNames!=null && !ExpressionAttributeNames.trim().equals("")){
            querySpec = querySpec.withNameMap(parseHashMap(parseJsonMap(ExpressionAttributeNames.trim())));
        }
        if(ExpressionAttributeValues!=null && !ExpressionAttributeValues.trim().equals("")){
            querySpec = querySpec.withValueMap(getValueMap(parseJsonMap(ExpressionAttributeValues.trim())));
        }
        if(ExclusiveStartKey!=null && !ExclusiveStartKey.trim().equals("")){
            querySpec.getRequest().setExclusiveStartKey(getAttributeKey(parseJsonMap(ExclusiveStartKey.trim())));
        }
        if(ReturnConsumedCapacitys!=null && !ReturnConsumedCapacitys.trim().equals("")){
            querySpec = querySpec.withReturnConsumedCapacity(ReturnConsumedCapacity.fromValue(ReturnConsumedCapacitys.trim()));
        }
        if(maxResultSize>0){
            querySpec = querySpec.withMaxResultSize(maxResultSize);
        }
        if(SelectS!=null && !SelectS.trim().equals("")){
            querySpec.withSelect(Select.fromValue(SelectS));
        }
        querySpec = querySpec.withScanIndexForward(ScanIndexForward).withConsistentRead(ConsistentRead);

        ItemCollection<QueryOutcome> items = null;
        try {
            items = table.query(querySpec);
            return getQueryResult(items);
        }
        catch (Exception e) {
            String message = e.getLocalizedMessage() + "\n" + "Error QuerySpec "+querySpec.getRequest();
            DSException.throwRuntime(new Throwable(message));
        }
        return new DSMap();
    }

    private static DSMap getQueryResult(ItemCollection<QueryOutcome> items){

        DSMap resultMap = new DSMap();
        Iterator<Item> iterator = items.iterator();
        DSList itemList = new DSList();
        while (iterator.hasNext()) {
            Item item = iterator.next();
            itemList.add(parseJsonMap(item.toJSON()));
        }
        resultMap.put("Items",itemList);
        resultMap.put("Count",items.getAccumulatedItemCount());
        resultMap.put("ScannedCount",items.getAccumulatedScannedCount());
        if(items.getLastLowLevelResult().getQueryResult().getConsumedCapacity()!=null){
            resultMap.put("ConsumedCapacity",items.getLastLowLevelResult().getQueryResult().getConsumedCapacity().toString());
        }

        if(items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey()!=null){
            // below logic needed to sync with standard AttributeValues format.
            // Note primary key will always have S, B N type so its safe to assume this
            Map<String,AttributeValue> lastKey = items.getLastLowLevelResult().getQueryResult().getLastEvaluatedKey();
            DSMap lastKeyMap = new DSMap();
            for (Map.Entry<String, AttributeValue> entry : lastKey.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();
                value= value.replaceFirst("\\{","{\"");
                value=value.replaceFirst(":","\":");
                value=value.replaceFirst(": ",":\"");
                value=value.replaceFirst(",}","\"}");
                lastKeyMap.put(key,parseJsonMap(value));
            }
            resultMap.put("LastEvaluatedKey",lastKeyMap);
        }
        return resultMap;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Put Item
    ///////////////////////////////////////////////////////////////////////////

    public static void putItem(DynamoDB client,
                               String tableName,
                               String itemsStr,
                               String conditionExpression,
                               String expressionAttributeNames,
                               String expressionAttributeValues,
                               String returnConsumedCapacitys,
                               String returnItemCollectionMetrics,
                               String returnValues){

        Table table = client.getTable(tableName);

        PutItemSpec putItemSpec = new PutItemSpec();
        Item item = Item.fromJSON(itemsStr);
        putItemSpec.withItem(item);
        if(conditionExpression!=null && !conditionExpression.trim().equals("")){
            putItemSpec = putItemSpec.withConditionExpression(conditionExpression);
        }
        if(expressionAttributeNames!=null && !expressionAttributeNames.trim().equals("")){
            putItemSpec = putItemSpec.withNameMap(parseHashMap(parseJsonMap(expressionAttributeNames.trim())));
        }
        if(expressionAttributeValues!=null && !expressionAttributeValues.trim().equals("")){
            putItemSpec = putItemSpec.withValueMap(getValueMap(parseJsonMap(expressionAttributeValues.trim())));
        }
        if(returnConsumedCapacitys!=null && !returnConsumedCapacitys.trim().equals("")){
            putItemSpec = putItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.fromValue(returnConsumedCapacitys.trim()));
        }
        if(returnItemCollectionMetrics!=null && !returnItemCollectionMetrics.trim().equals("")){
            putItemSpec = putItemSpec.withReturnItemCollectionMetrics(ReturnItemCollectionMetrics.fromValue(returnItemCollectionMetrics.trim()));
        }
        if(returnValues !=null && !returnValues.trim().equals("")){
            putItemSpec = putItemSpec.withReturnValues(ReturnValue.fromValue(returnValues));
        }

       try {
            PutItemOutcome putOutcome = table.putItem(putItemSpec);
        }
        catch (Exception e) {
            String message = e.getLocalizedMessage() + "\n" + "Error PutItem "+putItemSpec.getRequest();
            DSException.throwRuntime(new Throwable(message));
        }


    }

    ///////////////////////////////////////////////////////////////////////////
    // Batch Put Items
    ///////////////////////////////////////////////////////////////////////////

    public static String batchPutItems(DynamoDB client,
                               String tableName,
                               String itemsStr){
        DSLogger log = new DSLogger();
        DSElement element = parseJSON(itemsStr);
        if(!(element instanceof DSList)){
            DSException.throwRuntime(new Throwable("Error PutBatchItems: Not JSON Array " + element));
            return null;
        }
        TableWriteItems tableWriteItems = new TableWriteItems(tableName);
        ArrayList itemsToPut = new ArrayList();
        for (DSElement en : (DSList)element) {
            itemsToPut.add(Item.fromJSON(en.toString()));
            log.info(en);
        }
        tableWriteItems.withItemsToPut(itemsToPut);

        BatchWriteItemOutcome outcome = client.batchWriteItem(tableWriteItems);
        do {
            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

            if (outcome.getUnprocessedItems().size() > 0) {
                log.info("Retrieving the unprocessed items");
                outcome = client.batchWriteItemUnprocessed(unprocessedItems);
            }
            else {
                log.info("No unprocessed items found");
            }
        } while (outcome.getUnprocessedItems().size() > 0);

        return null;
    }


    ///////////////////////////////////////////////////////////////////////////
    // Scan Items
    ///////////////////////////////////////////////////////////////////////////

    public static String scanDynamodb(DynamoDB client,
                                      String tableName,
                                      String ProjectionExpression,
                                      int Limit,
                                      String FilterExpression,
                                      String ExpressionAttributeNames,
                                      String ExpressionAttributeValues,
                                      String select,
                                      boolean ConsistentRead,
                                      int Segment,
                                      int TotalSegments,
                                      String ExclusiveStartKey,
                                      String ReturnConsumedCapacitys){

        Table table = client.getTable(tableName);
        ScanSpec scanSpec = new ScanSpec();
        if(ProjectionExpression!=null && !ProjectionExpression.equals("")){
            scanSpec = scanSpec.withProjectionExpression(ProjectionExpression);
        }
        if(Limit > 0) {
            scanSpec = scanSpec.withMaxResultSize(Limit);
        }
        if(FilterExpression!=null && !FilterExpression.equals("")){
            scanSpec = scanSpec.withFilterExpression(FilterExpression) ;
        }
        if(ExpressionAttributeNames!=null && !ExpressionAttributeNames.equals("")){
            scanSpec = scanSpec.withNameMap(parseHashMap(parseJsonMap(ExpressionAttributeNames)));
        }
        if(ExpressionAttributeValues!=null && !ExpressionAttributeValues.equals("")){
            scanSpec = scanSpec.withValueMap(getValueMap(parseJsonMap(ExpressionAttributeValues)));
        }
        if(select!=null && !select.equals("")){
            scanSpec = scanSpec.withSelect(Select.fromValue(select));
        }
        scanSpec = scanSpec.withConsistentRead(ConsistentRead);
        if(Segment > 0){
            scanSpec = scanSpec.withSegment(Segment);
        }
        if(TotalSegments > 0){
            scanSpec = scanSpec.withTotalSegments(TotalSegments);
        }
        if(ExclusiveStartKey!=null && !ExclusiveStartKey.trim().equals("")){
            scanSpec.getRequest().setExclusiveStartKey(getAttributeKey(parseJsonMap(ExclusiveStartKey.trim())));
        }
        if(ReturnConsumedCapacitys!=null && !ReturnConsumedCapacitys.equals("")){
            scanSpec = scanSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.fromValue(ReturnConsumedCapacitys));
        }

        ItemCollection<ScanOutcome> items = null;
        try {
            items = table.scan(scanSpec);
            return getScanResult(items);
        }
        catch (Exception e) {
            String message = e.getLocalizedMessage() + "\n" + "Error ScanItem "+scanSpec.getRequest();
            DSException.throwRuntime(new Throwable(message));
        }
        return "";
    }

    private static String getScanResult(ItemCollection<ScanOutcome> items){

        DSMap resultMap = new DSMap();
        Iterator<Item> iterator = items.iterator();
        DSList itemList = new DSList();
        while (iterator.hasNext()) {
            Item item = iterator.next();
            itemList.add(parseJsonMap(item.toJSON()));
        }
        resultMap.put("Items",itemList);
        resultMap.put("Count",items.getAccumulatedItemCount());
        resultMap.put("ScannedCount",items.getAccumulatedScannedCount());
        if(items.getLastLowLevelResult().getScanResult().getConsumedCapacity()!=null){
            resultMap.put("ConsumedCapacity",items.getLastLowLevelResult().getScanResult().getConsumedCapacity().toString());
        }

        if(items.getLastLowLevelResult().getScanResult().getLastEvaluatedKey()!=null){
            // below logic needed to sync with standard AttributeValues format.
            // Note primary key will always have S, B N type so its safe to assume this
            Map<String,AttributeValue> lastKey = items.getLastLowLevelResult().getScanResult().getLastEvaluatedKey();
            DSMap lastKeyMap = new DSMap();
            for (Map.Entry<String, AttributeValue> entry : lastKey.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue().toString();
                value= value.replaceFirst("\\{","{\"");
                value=value.replaceFirst(":","\":");
                value=value.replaceFirst(": ",":\"");
                value=value.replaceFirst(",}","\"}");
                lastKeyMap.put(key,parseJsonMap(value));
            }
            resultMap.put("LastEvaluatedKey",lastKeyMap);
        }
        return resultMap.toString();
    }

    ///////////////////////////////////////////////////////////////////////////
    // Update Items
    ///////////////////////////////////////////////////////////////////////////

    public static String updateItem(DynamoDB client,
                                  String tableName,
                                  String primaryKey,
                                  String updateExpression,
                                  String conditionExpression,
                                  String expressionAttributeNames,
                                  String expressionAttributeValues,
                                  String returnConsumedCapacitys,
                                  String returnItemCollectionMetrics,
                                  String returnValues){

        Table table = client.getTable(tableName);

        UpdateItemSpec updateItemSpec = new UpdateItemSpec();
        updateItemSpec.withPrimaryKey(getPrimaryKey(parseJsonMap(primaryKey.trim())));
        if(updateExpression!=null && !updateExpression.trim().equals("")){
            updateItemSpec = updateItemSpec.withUpdateExpression(updateExpression);
        }
        if(conditionExpression!=null && !conditionExpression.trim().equals("")){
            updateItemSpec = updateItemSpec.withConditionExpression(conditionExpression);
        }
        if(expressionAttributeNames!=null && !expressionAttributeNames.trim().equals("")){
            updateItemSpec = updateItemSpec.withNameMap(parseHashMap(parseJsonMap(expressionAttributeNames.trim())));
        }
        if(expressionAttributeValues!=null && !expressionAttributeValues.trim().equals("")){
            updateItemSpec = updateItemSpec.withValueMap(getValueMap(parseJsonMap(expressionAttributeValues.trim())));
        }
        if(returnConsumedCapacitys!=null && !returnConsumedCapacitys.trim().equals("")){
            updateItemSpec = updateItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.fromValue(returnConsumedCapacitys.trim()));
        }
        if(returnItemCollectionMetrics!=null && !returnItemCollectionMetrics.trim().equals("")){
            updateItemSpec = updateItemSpec.withReturnItemCollectionMetrics(ReturnItemCollectionMetrics.fromValue(returnItemCollectionMetrics.trim()));
        }
        if(returnValues !=null && !returnValues.trim().equals("")){
            updateItemSpec = updateItemSpec.withReturnValues(ReturnValue.fromValue(returnValues));
        }

        try {
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            return updateItemOutcome.getItem().toJSONPretty();
        }
        catch (Exception e) {
            String message = e.getLocalizedMessage() + "\n" + "Error UpdateItem "+updateItemSpec.getRequest();
            DSException.throwRuntime(new Throwable(message));
        }
        return "";
    }

    ///////////////////////////////////////////////////////////////////////////
    // Delete Items
    ///////////////////////////////////////////////////////////////////////////

    public static String deleteItem(DynamoDB client,
                                  String tableName,
                                  String primaryKey,
                                  String conditionExpression,
                                  String expressionAttributeNames,
                                  String expressionAttributeValues,
                                  String returnConsumedCapacitys,
                                  String returnItemCollectionMetrics,
                                  String returnValues){

        Table table = client.getTable(tableName);

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec();
        deleteItemSpec.withPrimaryKey(getPrimaryKey(parseJsonMap(primaryKey.trim())));
        if(conditionExpression!=null && !conditionExpression.trim().equals("")){
            deleteItemSpec = deleteItemSpec.withConditionExpression(conditionExpression);
        }
        if(expressionAttributeNames!=null && !expressionAttributeNames.trim().equals("")){
            deleteItemSpec = deleteItemSpec.withNameMap(parseHashMap(parseJsonMap(expressionAttributeNames.trim())));
        }
        if(expressionAttributeValues!=null && !expressionAttributeValues.trim().equals("")){
            deleteItemSpec = deleteItemSpec.withValueMap(getValueMap(parseJsonMap(expressionAttributeValues.trim())));
        }
        if(returnConsumedCapacitys!=null && !returnConsumedCapacitys.trim().equals("")){
            deleteItemSpec = deleteItemSpec.withReturnConsumedCapacity(ReturnConsumedCapacity.fromValue(returnConsumedCapacitys.trim()));
        }
        if(returnItemCollectionMetrics!=null && !returnItemCollectionMetrics.trim().equals("")){
            deleteItemSpec = deleteItemSpec.withReturnItemCollectionMetrics(ReturnItemCollectionMetrics.fromValue(returnItemCollectionMetrics.trim()));
        }
        if(returnValues !=null && !returnValues.trim().equals("")){
            deleteItemSpec = deleteItemSpec.withReturnValues(ReturnValue.fromValue(returnValues));
        }

        try {
            DeleteItemOutcome deleteItemOutcome = table.deleteItem(deleteItemSpec);
            return deleteItemOutcome.getItem().toJSONPretty();
        }
        catch (Exception e) {
            String message = e.getLocalizedMessage() + "\n" + "Error Delete Item "+deleteItemSpec.getRequest();
            DSException.throwRuntime(new Throwable(message));
        }
        return "";
    }


    ///////////////////////////////////////////////////////////////////////////
    // Common functions
    ///////////////////////////////////////////////////////////////////////////

    public static ValueMap getValueMap(DSMap valueDSMap){
        ValueMap valueMap = new ValueMap();
        DSLogger log = new DSLogger();
        for (DSMap.Entry en : valueDSMap) {
            String key = en.getKey();
            if(en.getValue() instanceof DSMap){
                DSMap valueDetails = (DSMap)en.getValue();
                String valType = valueDetails.firstEntry().getKey();
                DSElement val = valueDetails.firstEntry().getValue();
                switch(valType){
                    case "B" :
                        valueMap.withBinary(key,val.toString().getBytes());
                        break;
                    case "BOOL" :
                        valueMap.withBoolean(key,Boolean.getBoolean(val.toString()));
                        break;
                    case "BS" :
                        valueMap.withBinarySet(key,getBibarySet((DSList)val));
                        break;
                     case "L" :
                        valueMap.withList(key,getList((DSList)val));
                        break;
                    case "M" :
                        valueMap.withMap(key,parseHashMap(val.toMap()));
                        break;
                    case "N" :
                        valueMap.withNumber(key,Double.valueOf(val.toString()));
                        break;
                    case "NS" :
                        valueMap.withNumberSet(key,getNumberSet((DSList)val));
                        break;
                    case "NULL" :
                        valueMap.withNull(key);
                        break;
                    case "S" :
                        valueMap.withString(key,val.toString());
                        break;
                     case "SS" :
                        valueMap.withStringSet(key,getStringSet((DSList)val));
                        break;
                    default :
                        log.error("Wrong data type for " + key);
                        break;
                }
            } else {
                log.error(" Wrong format for " + key);
            }
        }
        return valueMap;
    }

    public static List<String> getList(DSList list){
        List listA = new ArrayList();
        for (DSElement en : list) {
            if(en.getElementType().equals(DSValueType.NUMBER)){
                listA.add(Double.valueOf(en.toDouble()));
            }else {
                listA.add(en.toString());
            }
        }
        return listA;
    }

    private static Map<String, AttributeValue> getAttributeKey(DSMap keyMap){
        Map<String, AttributeValue> startKey = new HashMap<>();

        for (DSMap.Entry en : keyMap) {
            String key = en.getKey().trim();
            DSMap valueMaP = (DSMap)en.getValue();
            // Only S, N and B are allowed as primary keys
            if(valueMaP.firstEntry().getKey().equals("S")){
                startKey.put(key, new AttributeValue().withS(valueMaP.firstEntry().getValue().toString()));
            } else if(valueMaP.firstEntry().getKey().equals("N")){
                startKey.put(key, new AttributeValue().withN(valueMaP.firstEntry().getValue().toString()));
            } else if(valueMaP.firstEntry().getKey().equals("B")){
                ByteBuffer buffer = ByteBuffer.wrap((valueMaP.firstEntry().getValue().toString().getBytes()));
                startKey.put(key, new AttributeValue().withB(buffer));
            }
        }
        return startKey;
    }

    private static PrimaryKey getPrimaryKey(DSMap keyMap) {
        PrimaryKey primaryKey = new PrimaryKey();
        for (DSMap.Entry en : keyMap) {
            String key = en.getKey().trim();
            DSMap valueMaP = (DSMap)en.getValue();
            // Only S, N are allowed as primary keys
            if(valueMaP.firstEntry().getKey().equals("S")){
                primaryKey.addComponent(key, valueMaP.firstEntry().getValue().toString());
            } else if(valueMaP.firstEntry().getKey().equals("N")){
                primaryKey.addComponent(key, Long.parseLong(valueMaP.firstEntry().getValue().toString()));
            }
        }
        return primaryKey;
    }

    public static Set<byte[]> getBibarySet(DSList list){
        Set setC = new LinkedHashSet();
        for (DSElement en : list) {
            setC.add(en.toString().getBytes());
        }
        return setC;
    }

    public static Set<String> getStringSet(DSList list){
        Set setS = new LinkedHashSet();
        for (DSElement en : list) {
            setS.add(en.toString());
        }
        return setS;
    }

    public static Set<BigDecimal> getNumberSet(DSList list){
        Set setN = new LinkedHashSet();
        for (DSElement en : list) {
            setN.add(Double.valueOf(en.toString()));
        }
        return setN;
    }

    public static Map<String,String> parseHashMap(DSMap m){
        Map<String,String> map = new HashMap<>();
        for (DSMap.Entry en : m) {
            map.put(en.getKey().trim(), en.getValue().toString().trim());
        }
        return map;
    }

    public static DSMap parseJsonMap(String jsonValue){
        JsonReader reader = new JsonReader(jsonValue);
        DSMap m = reader.getMap();
        reader.close();
        return m;
    }

    private static DSElement parseJSON(String jsonValue){
        JsonReader reader = new JsonReader(jsonValue);
        DSElement m = reader.getElement();
        reader.close();
        return m;
    }

    public static DSFlexEnum getTableNames(DynamoDBDSAClient client) {
        List<String> dbTableList = client.listtable();
        DSList tableList = new DSList();
        for(String dtTableName : dbTableList) {
            tableList.add(dtTableName);
        }
        return DSFlexEnum.valueOf(tableList.get(0).toString(),tableList);
    }

    public static DSFlexEnum getRegions() {
        DSList regionList = new DSList();
        for (Regions region : Regions.values()) {
            regionList.add(region.getName());
        }
        return DSFlexEnum.valueOf(regionList.get(0).toString(),regionList);
    }
}
