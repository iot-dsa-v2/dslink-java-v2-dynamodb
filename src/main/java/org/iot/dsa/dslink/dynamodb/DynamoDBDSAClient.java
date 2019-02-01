package org.iot.dsa.dslink.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

import org.iot.dsa.node.DSMap;

import java.util.List;

public class DynamoDBDSAClient {

    private String accessKey = null;
    private String secretKey = null;
    private String region = null;
    private String endpoint = null;
    private AmazonDynamoDB client;

    public DynamoDBDSAClient(String accessKey, String secretKey, String region, String endpoint){
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.endpoint = endpoint;
        client = Util.connectDynamoDB(accessKey,secretKey,region,endpoint);
    }

    public void listtable(){
        ListTablesResult tables = client.listTables();
        List tnames = tables.getTableNames();
        for(int i = 0 ; i < tnames.size();i++){
            System.out.println(tnames.get(i));
        }
    }

    public DSMap QueryItem(DSMap parameters){
        String tableName = parameters.getString(Constants.TABLENAME);
        String projectionExpression = parameters.getString(Constants.PROJECTIONEXPRESSION);
        String keyConditionExpression = parameters.getString(Constants.KEYCONDITIONEXPRESSION);
        String filterExpression = parameters.getString(Constants.FILTEREXPRESSION);
        String expressionAttributeNames = parameters.getString(Constants.EXPRESSIONATTRIBUTENAMES);
        String expressionAttributeValues = parameters.getString(Constants.EXPRESSIONATTRIBUTEVALUES);
        String exclusiveStartKey = parameters.getString(Constants.EXCLUSIVESTARTKEY);;
        String select = parameters.getString(Constants.SELECT);
        int limit = parameters.getInt(Constants.LIMIT);
        boolean scanIndexForward = parameters.getBoolean(Constants.SCANINDEXFORWARD);
        boolean sonsistentRead = parameters.getBoolean(Constants.CONSISTENTREAD);
        String returnConsumedCapacity = parameters.getString(Constants.RETURNCONSUMESCAPACITY);

        return Util.queryDynamodb(client,tableName,
                projectionExpression,keyConditionExpression,
                filterExpression,expressionAttributeNames,
                expressionAttributeValues,exclusiveStartKey,select,
                limit,scanIndexForward,
                sonsistentRead,returnConsumedCapacity);
    }

    public String ScanItem(DSMap parameters){

        String tableName = parameters.getString(Constants.TABLENAME);
        String ProjectionExpression = parameters.getString(Constants.PROJECTIONEXPRESSION);
        int Limit = parameters.getInt(Constants.LIMIT);
        String FilterExpression = parameters.getString(Constants.FILTEREXPRESSION);
        String ExpressionAttributeNames = parameters.getString(Constants.EXPRESSIONATTRIBUTENAMES);
        String ExpressionAttributeValues = parameters.getString(Constants.EXPRESSIONATTRIBUTEVALUES);
        String Select = parameters.getString(Constants.SELECT);
        boolean ConsistentRead = parameters.getBoolean(Constants.CONSISTENTREAD);
        int Segment = parameters.getInt(Constants.SEGMENT);
        int TotalSegments = parameters.getInt(Constants.TOTALSEGMENTS);
        String ExclusiveStartKey = parameters.getString(Constants.EXCLUSIVESTARTKEY);
        String ReturnConsumedCapacity = parameters.getString(Constants.RETURNCONSUMESCAPACITY);

        return Util.scanDynamodb(new DynamoDB(client),tableName,ProjectionExpression,
                Limit,FilterExpression,ExpressionAttributeNames,
                ExpressionAttributeValues,Select, ConsistentRead,
                Segment,TotalSegments,ExclusiveStartKey, ReturnConsumedCapacity);
    }

    public String putItem(DSMap parameters){
        String tableName = parameters.getString(Constants.TABLENAME);
        String item = parameters.getString(Constants.ITEM);
        String conditionExpression = parameters.getString(Constants.CONDITIONEXPRESSION);
        String expressionAttributeNames = parameters.getString(Constants.EXPRESSIONATTRIBUTENAMES);
        String expressionAttributeValues = parameters.getString(Constants.EXPRESSIONATTRIBUTEVALUES);

        Util.putItem(new DynamoDB(client),
                tableName,
                item,
                conditionExpression,
                expressionAttributeNames,
                expressionAttributeValues,
                "TOTAL",
                "SIZE",
                "ALL_OLD");

        return "";
    }

    public String batchPutItem(DSMap parameters){
        String tableName = parameters.getString(Constants.TABLENAME);
        String items = parameters.getString(Constants.ITEMS);

        Util.batchPutItems(new DynamoDB(client),
                tableName,
                items);
        return "";
    }

    public String updateItem(DSMap parameters){
        String tableName = parameters.getString(Constants.TABLENAME);
        String primaryKey = parameters.getString(Constants.PRIMARYKEY);
        String updateExpression = parameters.getString(Constants.UPDATEEXPRESSION);
        String conditionExpression = parameters.getString(Constants.CONDITIONEXPRESSION);
        String expressionAttributeNames = parameters.getString(Constants.EXPRESSIONATTRIBUTENAMES);
        String expressionAttributeValues = parameters.getString(Constants.EXPRESSIONATTRIBUTEVALUES);

        return Util.updateItem(new DynamoDB(client),
                tableName,
                primaryKey,
                updateExpression,
                conditionExpression,
                expressionAttributeNames,
                expressionAttributeValues,
                "TOTAL",
                "SIZE",
                "ALL_NEW");
    }

    public String deleteItem(DSMap parameters){
        String tableName = parameters.getString(Constants.TABLENAME);
        String primaryKey = parameters.getString(Constants.PRIMARYKEY);
        String conditionExpression = parameters.getString(Constants.CONDITIONEXPRESSION);
        String expressionAttributeNames = parameters.getString(Constants.EXPRESSIONATTRIBUTENAMES);
        String expressionAttributeValues = parameters.getString(Constants.EXPRESSIONATTRIBUTEVALUES);

        return Util.deleteItem(new DynamoDB(client),
                tableName,
                primaryKey,
                conditionExpression,
                expressionAttributeNames,
                expressionAttributeValues,
                "TOTAL",
                "SIZE",
                "ALL_OLD");

    }
}
