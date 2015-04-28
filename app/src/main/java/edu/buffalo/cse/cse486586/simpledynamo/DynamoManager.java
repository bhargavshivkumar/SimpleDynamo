package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.os.Message;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by bhargav on 4/21/15.
 */
public class DynamoManager {


    private static final String REPLICA = "REPLICA";
    private static final String DELETE_REQUEST ="DELETE_REQUEST" ;
    private static final String INSERT_REQUEST = "INSERT_REQUEST";
    private static final String QUERY_REQUEST ="QUERY_REQUEST" ;
    private static final String SELECT_ALL ="SELECT_ALL" ;
    private static final String SYNCHRONIZE = "SYNCHRONIZE";
    LinkedList<Node> dynamoState;
    Node me;
    public static final String TAG = "Dynamo Manager";
    HashMap<String,String> hashTable;

    public DynamoManager()
    {


    }

    public void InitializeNode(String MyPort) {
        hashTable=new HashMap<>();
        dynamoState = new LinkedList<Node>();
        try {
            dynamoState.addFirst(new Node(genHash("5554"), "11112", "11116", "11108", "11120"));
            dynamoState.add(new Node(genHash("5558"), "11108", "11120", "11116", "11124"));
            dynamoState.add(new Node(genHash("5560"), "11116", "11124", "11120", "11112"));
            dynamoState.add(new Node(genHash("5562"), "11120", "11112", "11124", "11108"));
            dynamoState.addLast(new Node(genHash("5556"), "11124", "11108", "11112", "11116"));
        }
        catch (NoSuchAlgorithmException e)
        {
            Log.e(TAG,"No such algorithm exception");
        }

        for(Node n : dynamoState)
        {
            if(n.port.equals(MyPort))
            {
                me=n;
            }
        }
    }

    /*
    Node Structure
     */
    static class Node {

        String id;
        String pred;
        String succ;
        String port;
        String tail;

        public Node(String ids,String preds,String succs,String ports,String tails)
        {
            id=ids;
            pred=preds;
            succ=succs;
            port=ports;
            tail=tails;

        }

    }



    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Log.e(TAG,"Starting of server task");

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            //start code
            while(true) {
                try {


                    Socket clientSocket = serverSocket.accept();

                    Log.e(TAG,"Server task after client socket creation");



                    ObjectInputStream inFromClient= new ObjectInputStream(clientSocket.getInputStream());
                    MessageHandler objMessage= (MessageHandler)inFromClient.readObject();

                    //SimpleDhtActivity.var.WriteToUI(MessageHandler.ObjToString(objMessage) + "Port" + SimpleDhtActivity.myPort);


                   if(objMessage.MessageType.equalsIgnoreCase(INSERT_REQUEST))
                    {

                        boolean mine=CheckIfMine(objMessage.key);
                        if(mine) {
                            Log.d(TAG,"Inserting key:" +objMessage.key+" into hashtable");

                            //hashTable.put(objMessage.key, objMessage.value);
                            SimpleDynamoProvider.writeToDb(objMessage.key,objMessage.value,null);

                            Replicate(objMessage.key,objMessage.value,me);
                        }
                        else
                        {
                            Log.e(TAG,"Insert Forwarded to wrong owner");

                        }


                    }
                   else if(objMessage.MessageType.equalsIgnoreCase(REPLICA))
                   {
                       Log.d(TAG,"Inserting replica "+objMessage.key);
                       //hashTable.put(objMessage.key, objMessage.value);
                       SimpleDynamoProvider.writeToDb(objMessage.key,objMessage.value,REPLICA);

                   }
                   else if(objMessage.MessageType.equalsIgnoreCase(QUERY_REQUEST))
                   {
                       /* Querying the tail directly */
                        Cursor cur = SimpleDynamoProvider.ReadFromDb(objMessage.key);
                       MessageHandler replyObj = new MessageHandler();
                      // replyObj.cur=cur;


                       while(cur.moveToNext())
                       {
                           Log.d(TAG,cur.getString(1) + " Cursor string");
                           replyObj.table.put(objMessage.key,cur.getString(1));
                       }


                       SocketManager.WriteToSocket(clientSocket,replyObj);

                   }
                   else if(objMessage.MessageType.equalsIgnoreCase(SELECT_ALL))
                   {
                       Cursor cur = HandleSelectAt(objMessage.key);
                       MessageHandler replyObj = new MessageHandler();

                       while(cur.moveToNext()) {
                        replyObj.table.put(cur.getString(0),cur.getString(1));
                       }

                       SocketManager.WriteToSocket(clientSocket,replyObj);
                   }

                       else if(objMessage.MessageType.equalsIgnoreCase(DELETE_REQUEST))
                    {

                            int status = SimpleDynamoProvider.DeleteFromDB(objMessage.key);

                    }


                    clientSocket.close();



                } catch (Exception e) {
                    Log.e(TAG, "Exception in server socket");
                    e.printStackTrace();
                }
            }

            //end code


            //return null;
        }



    }



    public void SynchAfterRecover()
    {
        /* query one of 2 replicas to get all keys which are replicas*/
        MessageHandler objMessage=new MessageHandler();
        objMessage.MessageType=SYNCHRONIZE;
        Socket sock =SocketManager.OpenSocket(me.succ);
        SocketManager.WriteToSocket();
    }




    /*
    Check if key belongs to node in context
     */
    public boolean CheckIfMine(String key)
    {
        Log.e(TAG, "Inside Check if mine");
        Log.e(TAG,""+(me == null));
        Log.e(TAG,me.port+";"+me.pred+";"+me.succ+";"+key);
        try {

            String hashedKey = genHash(key);

            String predId=genHash(String.valueOf(Integer.parseInt(me.pred) / 2) );


            if((hashedKey.compareToIgnoreCase(predId) > 0 && hashedKey.compareToIgnoreCase(me.id) <=0)  ||
                    (predId.compareToIgnoreCase(me.id) >0 && (hashedKey.compareToIgnoreCase(predId) >0 || hashedKey.compareToIgnoreCase(me.id)<=0))
                    ||me.succ.equalsIgnoreCase(me.port))
            {
                Log.e(TAG,"It is mine");
                return true;
            }



        }
        catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG,"Problem generating Hash");
            e.printStackTrace();
        }
        catch (Exception ex)
        {
            Log.e(TAG,"Check if mine exception");
            ex.printStackTrace();
        }



        return false;
    }

    /* check if argument node owns the key*/
    public boolean CheckOwnership(Node owner,String key)
    {
        Log.e(TAG, "Inside Check Ownership");
        Log.e(TAG,""+(owner == null));
        Log.e(TAG,owner.port+";"+owner.pred+";"+owner.succ+";"+owner.id+";"+key);
        try {

            String hashedKey = genHash(key);

            String predId=genHash(String.valueOf(Integer.parseInt(owner.pred) / 2) );


            if((hashedKey.compareToIgnoreCase(predId) > 0 && hashedKey.compareToIgnoreCase(owner.id) <=0)  ||
                    (predId.compareToIgnoreCase(owner.id) >0 && (hashedKey.compareToIgnoreCase(predId) >0 || hashedKey.compareToIgnoreCase(owner.id)<=0)))
            {
                Log.e(TAG,"It is mine");
                return true;
            }



        }
        catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG,"Problem generating Hash");
            e.printStackTrace();
        }
        catch (Exception ex)
        {
            Log.e(TAG,"Check if mine exception");
            ex.printStackTrace();
        }



        return false;
    }



    public void HandleInsert(String key,String value)
    {
        Log.e(TAG,"Inside Handle Insert : " +key);
        if(CheckIfMine(key))
        {
            Log.d(TAG,"Adding to hashTable: "+key);
            //SimpleDhtActivity.var.WriteToUI("Inserting key:" +key+" into hashtable");
           // hashTable.put(key,value);
            SimpleDynamoProvider.writeToDb(key,value,null);
            Replicate(key,value,me);

        }
        else {

           Node owner= FindOwner(key);

            MessageHandler objMessage = new MessageHandler();
            objMessage.key=key;
            objMessage.value=value;
            objMessage.MessageType=INSERT_REQUEST;

            Socket sock = SocketManager.OpenSocket(owner.port);
            SocketManager.WriteToSocket(sock,objMessage);
            SocketManager.CloseSocket(sock);

            //handle Failure here. Wait for response from owner, else forward to owner's successor.
        }

    }

    private void Replicate(String key, String value, Node head) {

        MessageHandler objMessage =new MessageHandler();
        objMessage.MessageType = REPLICA;
        objMessage.key=key;
        objMessage.value=value;

        Log.d(TAG,"Writing Replica key : " + key + " to "+head.succ +" from "+head.port);
        /* propogate write to successor*/
        Socket sock =SocketManager.OpenSocket(head.succ);
        SocketManager.WriteToSocket(sock,objMessage);
        // I dont need to receive ack because if it fails it will forward to next replica server anyways
        SocketManager.CloseSocket(sock);


        Log.d(TAG,"Writing Replica key : " + key + " to "+head.tail +" from "+head.port);
        /* Propogate write to second replica*/
        Socket sock2=SocketManager.OpenSocket(head.tail);
        SocketManager.WriteToSocket(sock2,objMessage);
        SocketManager.CloseSocket(sock2);


    }



    public Cursor HandleQuery(String key)
    {
        Cursor dataCursor;
        Log.e(TAG,"Querying: "+key);
        if(CheckIfMine(key))
        {
            Log.e(TAG,"Inside handlequery checkifmine "+ key);



            dataCursor = SimpleDynamoProvider.ReadFromDb(key);
        }
        else
        {
            Node owner = FindOwner(key);
           dataCursor= QueryTail(owner,key);


        }



        return dataCursor;
    }

    private Cursor QueryTail(Node owner,String key) {

        MatrixCursor dataCursor = new MatrixCursor(new String[]{"key","value"});
        Log.e(TAG,"Forwarding request to Tail");
        MessageHandler objMess =new MessageHandler();
        objMess.MessageType=QUERY_REQUEST;
        objMess.key=key;

        Socket sock=SocketManager.OpenSocket(owner.tail);

        SocketManager.WriteToSocket(sock,objMess);

        MessageHandler newObj= SocketManager.ReadFromSocket(sock);
        SocketManager.CloseSocket(sock);

        dataCursor.addRow(new String[]{key,newObj.table.get(key)});

        Log.v(TAG,"Query returned : " +dataCursor.getCount());

       return dataCursor;

    }

    public Cursor HandleSelectAt(String key)
    {
        //Read from local db and pass all rows
        Cursor cur= SimpleDynamoProvider.ReadAllFromDb();
        return cur;
    }


    public Cursor HandleSelectAll(String key)
    {

        MatrixCursor dataCursor = new MatrixCursor(new String[]{"key","value"});
        MessageHandler objMessage=new MessageHandler();
        objMessage.MessageType=SELECT_ALL;
        Cursor cur=HandleSelectAt(key);

        while(cur.moveToNext())
        dataCursor.addRow(new String[]{cur.getString(0),cur.getString(1)});

        for(Node n: dynamoState)
        {
            if(n.port != me.port)
            {
                Socket sock = SocketManager.OpenSocket(n.port);
                SocketManager.WriteToSocket(sock,objMessage);
                MessageHandler newObj = SocketManager.ReadFromSocket(sock);
                SocketManager.CloseSocket(sock);

                /*populate the cursor with returned values */
                Iterator<String> it= newObj.table.keySet().iterator();
                while(it.hasNext())
                {
                    String KEY=it.next();
                    String VALUE=newObj.table.get(KEY);
                    dataCursor.addRow(new String[]{KEY,VALUE});

                    Log.d(TAG,"Key= "+KEY+"  VALUE= "+VALUE+"\n");

                }
            }
        }


        return dataCursor;


    }

    public int HandleDelete(String key)
    {
        //Delete from current node's database
        int status = SimpleDynamoProvider.DeleteFromDB(key);

        MessageHandler objMessage =new MessageHandler();
        objMessage.MessageType=DELETE_REQUEST;
        objMessage.key=key;

        for(Node n: dynamoState)
        {
            if(n.port != me.port) {
                Socket sock = SocketManager.OpenSocket(n.port);
                SocketManager.WriteToSocket(sock,objMessage);
                SocketManager.CloseSocket(sock);
            }
        }
        return status;
    }


    private Node FindOwner(String key) {


        for(Node n : dynamoState)
        {
            Log.e(TAG,"Looping through ring, node: "+ n.port);
                if(CheckOwnership(n,key))
                {
                    return n;
                }

        }

        Log.e(TAG,"No owner for key: " + key);
        return null;
    }



    /*
    Helper Functions
     */

    private String genHash(String input) throws NoSuchAlgorithmException {

        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String MathOperationsOnStrings(String str1,String str2, String operation)
    {
        String res ="";
        switch (operation)
        {
            case "DIVIDE":
                int num1= Integer.parseInt(str1);
                int num2 = Integer.parseInt(str2);
                int result= num1/num2;
                res= ""+result;
                break;
        }
        return res;
    }


}
