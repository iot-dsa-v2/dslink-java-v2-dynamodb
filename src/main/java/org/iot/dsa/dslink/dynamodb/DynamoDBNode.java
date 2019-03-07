package org.iot.dsa.dslink.dynamodb;

import org.iot.dsa.conn.DSBaseConnection;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSFlexEnum;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSInt;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec.ResultType;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSActionValues;
import org.iot.dsa.security.DSPasswordAes128;
import org.iot.dsa.util.DSException;

import java.util.Collection;

public class DynamoDBNode extends DSBaseConnection {

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    private final DSInfo access_key = getInfo(Constants.ACCESSKEY);
    private final DSInfo secret_key = getInfo(Constants.SECRETKEY);
    private final DSInfo region = getInfo(Constants.REGION);
    private final DSInfo endpoint = getInfo(Constants.ENDPOINT);

    //private DSMap parameters;
    private DynamoDBDSAClient client;

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    // Nodes must support the public no-arg constructor.  Technically this isn't required
    // since there are no other constructors...
    public DynamoDBNode(){

    }

    @Override
    protected void checkConfig() {
        if (access_key.getElement().toString().isEmpty()) {
            configFault("Empty Access Key");
            throw new IllegalStateException("Empty Access Key");
        }
        if (getSecretKey().trim().equals("")) {
            configFault("Empty Secret Key");
            throw new IllegalStateException("Empty Secret Key");
        }
        if (region.getElement().toString().isEmpty()) {
            configFault("Empty Region");
            throw new IllegalStateException("Empty Region");
        }else {
            try {
                com.amazonaws.regions.Regions.fromName(region.getElement().toString());
            } catch (IllegalArgumentException ex){
                configFault("Wrong Region");
                throw new IllegalStateException("Wrong Region");
            }
        }
        if (endpoint.getElement().toString().isEmpty()) {
            configFault("Empty End-Point");
            throw new IllegalStateException("Empty End-Point");
        }
        configOk();
    }

    public DynamoDBNode(DSMap parameters){
        setParameters(parameters);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Public Methods
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Override the method and add 'Edit Settings' action in default action group.
     */
    @Override
    public void getDynamicActions(DSInfo target, Collection<String> bucket) {
        if(target.getName().equals(this.getName())){
            bucket.add(Constants.EDITDYNAMODB);
        }
        super.getDynamicActions(target, bucket);
    }

    /**
     * Override the method and add 'Edit Settings' action in default action group.
     */
    @Override
    public DSInfo getDynamicAction(DSInfo target, String name) {
        if(name.equals(Constants.EDITDYNAMODB) && this.isStable()){
            DSInfo info = actionInfo(name, makeEditDynamoDBAction()) ;
            info.getMetadata().setActionGroup(DSAction.EDIT_GROUP, null);
            return info;
        }
        return super.getDynamicAction(target, name);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Protected Methods
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Defines the permanent children of this node type, their existence is guaranteed in all
     * instances.  This is only ever called once per, type per process.
     */
    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        // Add parameters.
        declareDefault(Constants.ACCESSKEY, DSString.valueOf("")).setReadOnly(true);
        declareDefault(Constants.SECRETKEY, DSPasswordAes128.NULL).setReadOnly(true).setHidden(true);
        declareDefault(Constants.REGION, Util.getRegions()).setReadOnly(true);
        declareDefault(Constants.ENDPOINT, DSString.valueOf("")).setReadOnly(true);
    }

    @Override
    protected void onStarted() {
        super.onStarted();
    }

    /**
     * Override this method and Initializes nodes and parameters once Node is Stable.
     */
    @Override
    protected void onStable() {
        init();
        enableDynamoDBActions();
    }

    protected void enableDynamoDBActions() {
        // Add actions
        put(Constants.QUERYDYNAMODB, makeQueryDynamoDBAction());
        put(Constants.SCANDYNAMODB, makeScanDynamoDBAction());
        put(Constants.PUTITEMDYNAMODB, makePutItemDynamoDBAction());
        put(Constants.BATCHPUTITEMSDYNAMODB, makeBatchPutItemDynamoDBAction());
        put(Constants.UPDATEITEMDYNAMODB, makeUpdateItemDynamoDBAction());
        put(Constants.DELETEITEMDYNAMODB, makeDeleteItemDynamoDBAction());
    }

    ///////////////////////////////////////////////////////////////////////////
    // Private Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Initializes DynamoDB Connection Node
     */
    private void init(){
        // Call this here in init which is called in onStable.
        // This is needed because parameter is not available at that time
        if (!canConnect()) {
            return;
        }
        try {
            client = new DynamoDBDSAClient(getAccessKey(),getSecretKey(),getRegion(),getEndpoint());
            client.listtable();
            connOk();
        } catch (Exception e) {
            configFault(e.getMessage().toString());
            throw new IllegalStateException(e.getMessage().toString());
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Make Action Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Make Edit Settings Action
     */
    private DSAction makeEditDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((DynamoDBNode) info.get()).editDynamoDB(invocation.getParameters());
                return null;
            }
        };
        act.addParameter(Constants.ACCESSKEY, DSString.valueOf(getAccessKey()), "AWS Access Key");
        act.addParameter(Constants.SECRETKEY, DSString.valueOf(getSecretKey()), "AWS Secret Key");
        act.addParameter(Constants.REGION, Util.getRegions(), "Region");
        act.addParameter(Constants.ENDPOINT, DSString.valueOf(getEndpoint()), "End Point");
        return act;
    }

    private DSAction makeQueryDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DynamoDBNode) info.get()).queryDynamoDB(this,invocation.getParameters());

            }
        };

        act.addParameter(Constants.TABLENAME, Util.getTableNames(client), "Table Name");
        act.addParameter(Constants.PROJECTIONEXPRESSION, DSValueType.STRING, "ProjectionExpression");
        act.addParameter(Constants.KEYCONDITIONEXPRESSION, DSValueType.STRING, "KeyConditionExpression");
        act.addParameter(Constants.FILTEREXPRESSION, DSValueType.STRING, "FilterExpression");
        act.addParameter(Constants.EXPRESSIONATTRIBUTENAMES, DSValueType.STRING, "ExpressionAttributeNames");
        act.addParameter(Constants.EXPRESSIONATTRIBUTEVALUES, DSValueType.STRING, "ExpressionAttributeValues");
        act.addParameter(Constants.EXCLUSIVESTARTKEY, DSValueType.STRING, "ExclusiveStartKey");
        DSList selectList = new DSList().add("").add("ALL_ATTRIBUTES")
                .add("ALL_PROJECTED_ATTRIBUTES").add("COUNT")
                .add("SPECIFIC_ATTRIBUTES");
        DSFlexEnum selectEnum = DSFlexEnum.valueOf("",selectList);
        act.addParameter(Constants.SELECT, selectEnum, "Select");
        act.addParameter(Constants.LIMIT, DSInt.valueOf(0), "Limit");
        act.addParameter(Constants.SCANINDEXFORWARD, DSBool.valueOf(true), "ScanIndexForward");
        act.addParameter(Constants.CONSISTENTREAD, DSBool.valueOf(false), "ConsistentRead");
        DSList rtnCapacityList = new DSList().add("").add("NONE").add("TOTAL").add("INDEXES");
        DSFlexEnum rtnCapacity = DSFlexEnum.valueOf("",rtnCapacityList);
        act.addParameter(Constants.RETURNCONSUMESCAPACITY, rtnCapacity, "ReturnConsumedCapacity");
        act.setResultType(ResultType.VALUES);
        act.addColumnMetadata(Constants.RESULT, DSValueType.STRING);
        return act;
    }

    private DSAction makePutItemDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DynamoDBNode) info.get()).putItem(this,invocation.getParameters());
            }
        };
        act.addParameter(Constants.TABLENAME, Util.getTableNames(client), "Table Name");
        act.addParameter(Constants.ITEM, DSValueType.STRING, "Item");
        act.addParameter(Constants.CONDITIONEXPRESSION, DSValueType.STRING, "ConditionExpression");
        act.addParameter(Constants.EXPRESSIONATTRIBUTENAMES, DSValueType.STRING, "Expression Attribute Names");
        act.addParameter(Constants.EXPRESSIONATTRIBUTEVALUES, DSValueType.STRING, "Expression Attribute Values");
        act.setResultType(ResultType.VALUES);
        act.addColumnMetadata(Constants.RESULT, DSValueType.STRING);
        return act;
    }

    private DSAction makeBatchPutItemDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DynamoDBNode) info.get()).batchPutItem(this,invocation.getParameters());
            }
        };
        act.addParameter(Constants.TABLENAME, Util.getTableNames(client), "Table Name");
        act.addParameter(Constants.ITEMS, DSValueType.STRING, "Items");
        act.setResultType(ResultType.VALUES);
        act.addColumnMetadata(Constants.RESULT, DSValueType.STRING);
        return act;
    }

    private DSAction makeScanDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DynamoDBNode) info.get()).scanDynamoDB(this,invocation.getParameters());
            }
        };
        act.addParameter(Constants.TABLENAME, Util.getTableNames(client), "Table Name");
        act.addParameter(Constants.PROJECTIONEXPRESSION, DSValueType.STRING, "ProjectionExpression");
        act.addParameter(Constants.LIMIT, DSInt.valueOf(0), "Limit");
        act.addParameter(Constants.FILTEREXPRESSION, DSValueType.STRING, "FilterExpression");
        act.addParameter(Constants.EXPRESSIONATTRIBUTENAMES, DSValueType.STRING, "ExpressionAttributeNames");
        act.addParameter(Constants.EXPRESSIONATTRIBUTEVALUES, DSValueType.STRING, "ExpressionAttributeValues");
        DSList selectList = new DSList().add("ALL_ATTRIBUTES").add("ALL_PROJECTED_ATTRIBUTES").add("SPECIFIC_ATTRIBUTES").add("COUNT");
        DSFlexEnum selectCapacity = DSFlexEnum.valueOf("ALL_ATTRIBUTES",selectList);
        act.addParameter(Constants.SELECT, selectCapacity, "Select");
        DSList rtnCapacityList = new DSList().add("NONE").add("TOTAL").add("INDEXES");
        DSFlexEnum rtnCapacity = DSFlexEnum.valueOf("NONE",rtnCapacityList);
        act.addParameter(Constants.CONSISTENTREAD, DSBool.valueOf(false), "ConsistentRead");
        act.addParameter(Constants.SEGMENT, DSInt.valueOf(0), "Segment");
        act.addParameter(Constants.TOTALSEGMENTS, DSInt.valueOf(0), "TotalSegments");
        act.addParameter(Constants.EXCLUSIVESTARTKEY, DSValueType.STRING, "ExclusiveStartKey");
        act.addParameter(Constants.RETURNCONSUMESCAPACITY, rtnCapacity, "ReturnConsumedCapacity");
        act.setResultType(ResultType.VALUES);
        act.addColumnMetadata(Constants.RESULT, DSValueType.STRING);
        return act;
    }

    private DSAction makeUpdateItemDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DynamoDBNode) info.get()).updateItem(this,invocation.getParameters());
            }
        };
        act.addParameter(Constants.TABLENAME, Util.getTableNames(client), "Table Name");
        act.addParameter(Constants.PRIMARYKEY, DSValueType.STRING, "Primary Key");
        act.addParameter(Constants.UPDATEEXPRESSION, DSValueType.STRING, "UpdateExpression");
        act.addParameter(Constants.CONDITIONEXPRESSION, DSValueType.STRING, "ConditionExpression");
        act.addParameter(Constants.EXPRESSIONATTRIBUTENAMES, DSValueType.STRING, "Expression Attribute Names");
        act.addParameter(Constants.EXPRESSIONATTRIBUTEVALUES, DSValueType.STRING, "Expression Attribute Values");
        act.setResultType(ResultType.VALUES);
        act.addColumnMetadata(Constants.RESULT, DSValueType.STRING);
        return act;
    }

    private DSAction makeDeleteItemDynamoDBAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DynamoDBNode) info.get()).deleteItem(this,invocation.getParameters());
            }
        };
        // Add parameters as needed
        act.addParameter(Constants.TABLENAME, Util.getTableNames(client), "Table Name");
        act.addParameter(Constants.PRIMARYKEY, DSValueType.STRING, "Primary Key");
        act.addParameter(Constants.CONDITIONEXPRESSION, DSValueType.STRING, "ConditionExpression");
        act.addParameter(Constants.EXPRESSIONATTRIBUTENAMES, DSValueType.STRING, "Expression Attribute Names");
        act.addParameter(Constants.EXPRESSIONATTRIBUTEVALUES, DSValueType.STRING, "Expression Attribute Values");
        act.setResultType(ResultType.VALUES);
        act.addColumnMetadata(Constants.RESULT, DSValueType.STRING);
        return act;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Actions
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Edit Dynamodb Settings
     */
    private void editDynamoDB(DSMap parameters){
        setParameters(parameters);
        debug("Account Edited" + getInfo().getName());
        init();
    }

    /**
     * Query Dynamodb
     */
    private ActionResult queryDynamoDB(DSAction action,DSMap parameters){
        DSActionValues actionResult = new DSActionValues(action);
        try {
            DSMap queryResult = client.QueryItem(parameters);
            actionResult.addResult(DSString.valueOf(new String(queryResult.toString())));
            connOk();
            return actionResult;
        } catch (Exception e){
            error(e.getLocalizedMessage());
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
            actionResult.addResult(DSString.valueOf(""));
            return actionResult;
        }
    }

    /**
     * Scan Item in
     */
    private ActionResult scanDynamoDB(DSAction action,DSMap parameters){
        DSActionValues actionResult = new DSActionValues(action);
        try {
            String queryResult = client.ScanItem(parameters);
            actionResult.addResult(DSString.valueOf(new String(queryResult)));
            connOk();
            return actionResult;
        } catch (Exception e){
            error(e.getLocalizedMessage());
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
            actionResult.addResult(DSString.valueOf(""));
            return actionResult;
        }
    }

    /**
     * Put Item. Return the put Item
     */
    private ActionResult putItem(DSAction action,DSMap parameters){
        DSActionValues actionResult = new DSActionValues(action);
        try {
            client.putItem(parameters);
            actionResult.addResult(parameters.get(Constants.ITEM));
            connOk();
            return actionResult;
        } catch (Exception e){
            error(e.getLocalizedMessage());
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
            actionResult.addResult(DSString.valueOf(""));
            return actionResult;
        }
    }

    /**
     * Batch Put Item
     */
    private ActionResult batchPutItem(DSAction action,DSMap parameters){
        DSActionValues actionResult = new DSActionValues(action);
        try {
            client.batchPutItem(parameters);
            actionResult.addResult(parameters.get(Constants.ITEMS));
            connOk();
            return actionResult;
        } catch (Exception e){
            error(e.getLocalizedMessage());
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
            actionResult.addResult(DSString.valueOf(""));
            return actionResult;
        }
    }

    /**
     * Update Item in
     */
    private ActionResult updateItem(DSAction action,DSMap parameters){
        DSActionValues actionResult = new DSActionValues(action);
        try {
            String updateResult = client.updateItem(parameters);
            actionResult.addResult(DSString.valueOf(updateResult));
            connOk();
            return actionResult;
        } catch (Exception e){
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
            actionResult.addResult(DSString.valueOf(""));
            return actionResult;
        }
    }

    /**
     * Update Item in
     */
    private ActionResult deleteItem(DSAction action,DSMap parameters){
        DSActionValues actionResult = new DSActionValues(action);
        try {
            String deleteResult = client.deleteItem(parameters);
            actionResult.addResult(DSString.valueOf(deleteResult));
            connOk();
            return actionResult;
        } catch (Exception e){
            error(e.getLocalizedMessage());
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
            actionResult.addResult(DSString.valueOf(""));
            return actionResult;
        }
    }


    private void setParameters(DSMap params) {
        if (!params.isNull(Constants.ACCESSKEY)) {
            put(access_key, params.get(Constants.ACCESSKEY));
        }
        if (!params.isNull(Constants.SECRETKEY)) {
            String pass = params.get(Constants.SECRETKEY).toString();
            put(secret_key, DSPasswordAes128.valueOf(pass));
        }
        if (!params.isNull(Constants.REGION)) {
            put(region, params.get(Constants.REGION));
        }
        if (!params.isNull(Constants.ENDPOINT)) {
            put(endpoint, params.get(Constants.ENDPOINT));
        }
    }
    private String getAccessKey() {
        return access_key.getValue().toString();
    }

    private String getSecretKey() {
        if(!secret_key.getValue().isNull()) {
            return ((DSPasswordAes128) secret_key.getValue()).decode();
        }else{
            return "";
        }
    }

    private String getRegion() {
        return region.getValue().toString();
    }

    private String getEndpoint() {
        return endpoint.getValue().toString();
    }
}
