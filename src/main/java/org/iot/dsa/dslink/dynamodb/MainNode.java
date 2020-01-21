package org.iot.dsa.dslink.dynamodb;

import org.iot.dsa.dslink.jdbc.AbstractMainNode;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.util.DSException;

public class MainNode extends AbstractMainNode {

    ///////////////////////////////////////////////////////////////////////////
    // Protected Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    protected DSAction makeAddDatabaseAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((MainNode) info.get()).connectDynamoDB(invocation.getParameters());
                return null;
            }
        };
        act.addParameter(Constants.CONNECTIONNAME, DSValueType.STRING, "Connection Name");
        act.addParameter(Constants.ACCESSKEY, DSValueType.STRING, "AWS Access Key");
        act.addParameter(Constants.SECRETKEY, DSValueType.STRING, "AWS Secret Key");
        act.addParameter(Constants.REGION, Util.getRegions(), "Region");
        act.addParameter(Constants.ENDPOINT, DSValueType.STRING, "End Point");

        return act;
    }

    protected String getHelpUrl() {
        return Constants.DYNAMODBDSLINKDOC;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Private Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Connect DynamoDB and add child Node on successful connection
     */
    private void connectDynamoDB(DSMap parameters){
        String name = parameters.getString(Constants.CONNECTIONNAME);
        try{
            put(name, new DynamoDBNode(parameters));
            info("Added connection " + name);
        } catch (Exception e){
            error(e.getLocalizedMessage());
            DSException.throwRuntime(new Throwable(e.getLocalizedMessage()));
        }
    }

}
