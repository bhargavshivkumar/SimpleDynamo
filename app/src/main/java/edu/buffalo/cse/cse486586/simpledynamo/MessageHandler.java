package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by bhargav on 4/21/15.
 */
public class MessageHandler implements Serializable {

    String myport;
    String MessageType;
    String nodeId;
    String succ;
    String pred;
    String key;
    String value;
    HashMap<String,String> table=new HashMap<String,String>();


    static String ObjToString(MessageHandler obj)
    {
        String compressObj= obj.myport + "=;" + obj.MessageType + "=;" + obj.nodeId + "=;" +obj.succ +"=;" +obj.pred+"=;"+obj.key;
        return compressObj;

    }

    static MessageHandler StringToObj(String mess)
    {
        MessageHandler objMessage =new MessageHandler();
        objMessage.myport=mess.split("=;")[0];
        objMessage.MessageType=mess.split("=;")[1];
        objMessage.nodeId =mess.split("=;")[2];
        objMessage.succ= mess.split("=;")[3];
        objMessage.pred= mess.split("=;")[4];
        return objMessage;
    }

}
