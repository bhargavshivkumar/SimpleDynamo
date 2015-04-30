package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.os.Message;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
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
    private static final String SYNCHRONIZE_REPLICA = "SYNCHRONIZE_REPLICA";
    private static final String SYNCHRONIZE_REPLICA_F1 = "SYNCHRONIZE_REPLICA_F1";
    private static final String SYNCHRONIZE_REPLICA_F2 = "SYNCHRONIZE_REPLICA_F2";
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

        /*TODO Create client task*/

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "", MyPort);
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




    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            SynchAfterRecover();
            return null;
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
                       SimpleDynamoProvider.writeToDb(objMessage.key,objMessage.value,objMessage.myport);

                   }
                   else if(objMessage.MessageType.equalsIgnoreCase(QUERY_REQUEST))
                   {
                       /* Querying the tail directly */
                        Cursor cur = SimpleDynamoProvider.ReadFromDb(objMessage.key);
                       MessageHandler replyObj = new MessageHandler();
                      // replyObj.cur=cur;

                       /*COPY CURSOR TO HASHMAP */
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
                    else if(objMessage.MessageType.equalsIgnoreCase(SYNCHRONIZE))
                   {
                       Log.d(TAG,"In server task synchronize method");
                        /*get keys from caller's replica nodes*/
                       Cursor cur= SimpleDynamoProvider.ReadReplicasFromDb(objMessage.myport);
                       MessageHandler replyObj = new MessageHandler();

                       while(cur.moveToNext())
                       {
                           Log.d(TAG,cur.getString(1) + " Cursor string");
                           replyObj.table.put(cur.getString(0),cur.getString(1));
                       }

                       SocketManager.WriteToSocket(clientSocket,replyObj);
                   }
                    else if(objMessage.MessageType.equalsIgnoreCase(SYNCHRONIZE_REPLICA))
                   {
                       /* get keys from  nodes for which the caller is a replica */
                        Cursor cur = SimpleDynamoProvider.RepopulateReplicasFromOwnerDbs();
                       MessageHandler replyObj = new MessageHandler();

                       while(cur.moveToNext())
                       {
                           replyObj.table.put(cur.getString(0),cur.getString(1));
                       }
                       SocketManager.WriteToSocket(clientSocket,replyObj);
                   }
                    else if(objMessage.MessageType.equalsIgnoreCase(SYNCHRONIZE_REPLICA_F1))
                   {
                       /*get keys that belong to my pred's pred whose replicas i store */
                       Cursor cur= SimpleDynamoProvider.ReadReplicasFromDb(objMessage.myport);
                       MessageHandler replyObj = new MessageHandler();

                       while(cur.moveToNext())
                       {
                           Log.d(TAG,cur.getString(1) + " Cursor string");
                           replyObj.table.put(cur.getString(0),cur.getString(1));
                       }

                       SocketManager.WriteToSocket(clientSocket,replyObj);

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

        SimpleDynamoActivity.var.WriteToUI("Beginning Recovery...\n\n");
        /* get own keys from replication nodes.
        deletes current db entries before adding to handle delete corner case
         */
        GetOwnKeys();


        /*  Getting  Data from predecessors */
        GetKeysfromPredecessor();

         /*Get data from pred's pred*/
        GetKeysFromPrePred();



    }

    public void GetKeysFromPrePred() {
        String ppred="";
        for(Node n: dynamoState)
        {
            if(n.port == me.pred)
            {
                ppred = n.pred;
            }
        }

        MessageHandler newObj2=null;
        try {
            SimpleDynamoActivity.var.WriteToUI("Querying my Predecessor's Predecessor "+ MathOperationsOnStrings(ppred,"2","DIVIDE")+" ...\n\n");
            MessageHandler objmess = new MessageHandler();
            objmess.MessageType = SYNCHRONIZE_REPLICA;

            Socket sock2 = SocketManager.OpenSocket(ppred);
            if(sock2 !=null) {
                SocketManager.WriteToSocket(sock2, objmess);
                newObj2 = SocketManager.ReadFromSocket(sock2);
                SocketManager.CloseSocket(sock2);
            }
            SimpleDynamoActivity.var.WriteToUI("SUCCESS!");
        }
        catch (IOException e)
        {
            /* if it fails, get from my predecessor */
            try
            {
                SimpleDynamoActivity.var.WriteToUI("Pre pred querying failed ...\n\n");
                SimpleDynamoActivity.var.WriteToUI("Querying my Predecessor "+ MathOperationsOnStrings(me.pred,"2","DIVIDE")+" ...\n\n");
                MessageHandler objmess = new MessageHandler();
                objmess.MessageType = SYNCHRONIZE_REPLICA_F1;
                objmess.myport=ppred;
                Socket sock2 = SocketManager.OpenSocket(me.pred);
                if(sock2 !=null) {
                    SocketManager.WriteToSocket(sock2, objmess);
                    newObj2 = SocketManager.ReadFromSocket(sock2);
                    SocketManager.CloseSocket(sock2);
                }
                SimpleDynamoActivity.var.WriteToUI("SUCCESS!");
            }
            catch(IOException exx) {
                SimpleDynamoActivity.var.WriteToUI("That failed too ...\n\n");
                Log.e(TAG, "Unable to synchronize replicas of pred's pred");
                e.printStackTrace();
            }
        }

        /* iterate through hashmap and write values to DB */
        if(newObj2 !=null)
        {
            /* The value for replica column will be the port of my predecessor*/
            Iterator<String> it = newObj2.table.keySet().iterator();
            while (it.hasNext()) {
                String KEY = it.next();
                String VALUE = newObj2.table.get(KEY);
                Log.d(TAG,"Writing key "+KEY+" to DB from hashmap");
                SimpleDynamoProvider.writeToDb(KEY, VALUE, ppred);

            }
        }
    }

    public void GetKeysfromPredecessor() {
    /* Get data from predecessor.*/
        MessageHandler newObj=null;
        try {
            SimpleDynamoActivity.var.WriteToUI("Querying my Predecessor "+ MathOperationsOnStrings(me.pred,"2","DIVIDE")+" ...\n\n");
            MessageHandler objmess = new MessageHandler();
            objmess.MessageType = SYNCHRONIZE_REPLICA;
            Socket sock2 = SocketManager.OpenSocket(me.pred);
            if(sock2 !=null) {
                SocketManager.WriteToSocket(sock2, objmess);
                newObj = SocketManager.ReadFromSocket(sock2);
                SocketManager.CloseSocket(sock2);
            }
            SimpleDynamoActivity.var.WriteToUI("SUCCESS!");
        }
        catch (IOException e)
        {
            Log.e(TAG," Predecessor Querying Failed");
            e.printStackTrace();
            /*take from my succ which is a replica for my pred */

            try
            {
                SimpleDynamoActivity.var.WriteToUI("Predecessor Query failed ...\n\n");
                SimpleDynamoActivity.var.WriteToUI("Querying my successor "+ MathOperationsOnStrings(me.succ,"2","DIVIDE")+" ...\n\n");
                MessageHandler objmess = new MessageHandler();
                objmess.MessageType = SYNCHRONIZE_REPLICA_F1;
                objmess.myport=me.pred;
                Socket sock2 = SocketManager.OpenSocket(me.succ);
                if(sock2 !=null) {
                    SocketManager.WriteToSocket(sock2, objmess);
                    newObj = SocketManager.ReadFromSocket(sock2);
                    SocketManager.CloseSocket(sock2);
                }
                SimpleDynamoActivity.var.WriteToUI("SUCCESS!");
            }
            catch(IOException exx) {
                SimpleDynamoActivity.var.WriteToUI("That failed too...\n\n");
                Log.e(TAG, "Unable to synchronize replicas of pred");
                exx.printStackTrace();
            }

        }


        /* iterate through hashmap and write values to DB */
        if(newObj !=null)
        {
            /* The value for replica column will be the port of my predecessor*/
            Iterator<String> it = newObj.table.keySet().iterator();
            while (it.hasNext()) {
                String KEY = it.next();
                String VALUE = newObj.table.get(KEY);
                Log.d(TAG,"Writing key "+KEY+" to DB from hashmap");
                SimpleDynamoProvider.writeToDb(KEY, VALUE, me.pred);

            }
        }
    }

    public void GetOwnKeys() {
        MessageHandler replyObj=null;
        /* query one of 2 replicas to get all keys which are replicas*/
        try {

            SimpleDynamoActivity.var.WriteToUI("Querying my successor "+ MathOperationsOnStrings(me.succ,"2","DIVIDE")+" ...\n\n");
            Log.d(TAG,"Inside Getownkeys()");
            MessageHandler objMessage = new MessageHandler();
            objMessage.MessageType = SYNCHRONIZE;
            objMessage.myport=me.port;
            Socket sock = SocketManager.OpenSocket(me.succ);
            if(sock != null) {
                SocketManager.WriteToSocket(sock, objMessage);
                replyObj = SocketManager.ReadFromSocket(sock);
                SocketManager.CloseSocket(sock);
                Log.d(TAG,"Successfully got own key list");
            }
            SimpleDynamoActivity.var.WriteToUI("SUCCESS!");
        }
        catch(IOException e)
        {
            /* If there is no response then query second replica */
            try {
                SimpleDynamoActivity.var.WriteToUI("Successor Query Failed ...\n\n");
                SimpleDynamoActivity.var.WriteToUI("Querying my Tail "+ MathOperationsOnStrings(me.tail,"2","DIVIDE")+" ...\n\n");
                MessageHandler objMessage = new MessageHandler();
                objMessage.MessageType = SYNCHRONIZE;
                objMessage.myport=me.port;
                Socket sock = SocketManager.OpenSocket(me.tail);
                if(sock != null) {
                    SocketManager.WriteToSocket(sock, objMessage);
                    replyObj = SocketManager.ReadFromSocket(sock);
                    SocketManager.CloseSocket(sock);
                }
                SimpleDynamoActivity.var.WriteToUI("SUCCESS!");
            }
            catch (IOException ex)
            {
                /* This shouldn't happen as there is only one failure at a time */
                SimpleDynamoActivity.var.WriteToUI("That failed too ...\n\n");
                Log.e(TAG, "Couldn't synch from either replica");
                e.printStackTrace();
            }

        }

        if(replyObj !=null) {
        /* delete everything in your database so that you have only whatever is stored on your replicas.
        * this handles the case when the key is deleted from your replica when you were down*/

            //HandleDelete("@");

        /*iterate through returned hashmap and Write the values to db */

            Iterator<String> it = replyObj.table.keySet().iterator();
            while (it.hasNext()) {
                String KEY = it.next();
                String VALUE = replyObj.table.get(KEY);
                Log.d(TAG,"Writing key "+KEY+" to DB from hashmap");
                SimpleDynamoProvider.writeToDb(KEY, VALUE, null);

            }
        }
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


            try {
                Socket sock = SocketManager.OpenSocket(owner.port);
                SocketManager.WriteToSocket(sock, objMessage);
                SocketManager.CloseSocket(sock);
            }
            catch (IOException e)
            {
                /* If  owner is down, send it to owner's successor, basically replicate it yourself*/

                Replicate(key,value,owner);

            }


        }

    }

    private void Replicate(String key, String value, Node head) {

        MessageHandler objMessage =new MessageHandler();
        objMessage.MessageType = REPLICA;
        objMessage.key=key;
        objMessage.value=value;
        objMessage.myport=head.port;


        try {

            Log.d(TAG, "Replicating Node " + head.port);
            Log.d(TAG, "Writing to port's successor "+head.succ);
        /* propogate write to successor*/
            Socket sock = SocketManager.OpenSocket(head.succ);
            SocketManager.WriteToSocket(sock, objMessage);
            // I dont need to receive ack because if it fails it will forward to next replica server anyways
            SocketManager.CloseSocket(sock);
        }
        catch (IOException e)
        {
            /* Dont need to handle anything as if there is a failure while replicating there isn't anything you can do as you are writing to two replicas */
            Log.e(TAG,"Socket exception in Replica function");
            e.printStackTrace();
        }

        try
        {


            Log.d(TAG, "Replicating Node " + head.port);
            Log.d(TAG, "Writing to port's Tail "+head.tail);
        /* Propogate write to second replica*/
            Socket sock2 = SocketManager.OpenSocket(head.tail);
            SocketManager.WriteToSocket(sock2, objMessage);
            SocketManager.CloseSocket(sock2);
        }
        catch (IOException e)
        {
            /* Dont need to handle anything as if there is a failure while replicating there isn't anything you can do as you are writing to two replicas */
         Log.e(TAG,"Socket exception in Replica function");
            e.printStackTrace();
        }

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
        MessageHandler newObj=null;


        if(owner.tail == me.port)
        {
                    /* If the tail is the cooridnator node, no need to forward the request */

            Cursor cur = SimpleDynamoProvider.ReadFromDb(key);


                       /*COPY CURSOR TO HASHMAP */
            while(cur.moveToNext())
            {
                Log.d(TAG,cur.getString(1) + " Cursor string");
                newObj=new MessageHandler();
                newObj.table.put(key,cur.getString(1));
            }
        }
        else {

            try {

                Socket sock = SocketManager.OpenSocket(owner.tail);

                SocketManager.WriteToSocket(sock, objMess);

                newObj = SocketManager.ReadFromSocket(sock);
                SocketManager.CloseSocket(sock);
            } catch (IOException e) {
            /* if tail is down, talk to the successor */

                Log.e(TAG, "Query Tail failed hence querying successor:" + owner.succ + " with key: " + key);

                if (owner.succ == me.port) {
                    /* If the successor is the cooridnator node, no need to forward the request */

                    Cursor cur = SimpleDynamoProvider.ReadFromDb(key);


                       /*COPY CURSOR TO HASHMAP */
                    while (cur.moveToNext()) {
                        Log.d(TAG, cur.getString(1) + " Cursor string");
                        newObj=new MessageHandler();
                        newObj.table.put(key, cur.getString(1));
                    }
                } else {

                    try {
                        Log.d(TAG,"Owner node: "+owner.port + " 's succ is not same as ME: "+me.port+". thus forwarding request to succ");
                        Socket sock = SocketManager.OpenSocket(owner.succ);

                        SocketManager.WriteToSocket(sock, objMess);

                        newObj = SocketManager.ReadFromSocket(sock);
                        SocketManager.CloseSocket(sock);
                    } catch (IOException ex) {
                /* shouldn't happen as there cannot be two failures at a time */
                        Log.e(TAG, "Two failures while query tail");
                        ex.printStackTrace();
                    }
                }

            }
        }

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
                MessageHandler newObj=null;
                try {
                    Socket sock = SocketManager.OpenSocket(n.port);
                    SocketManager.WriteToSocket(sock, objMessage);
                     newObj = SocketManager.ReadFromSocket(sock);
                    SocketManager.CloseSocket(sock);
                }
                catch (IOException e)
                {
                    /*TODO doesn't require special handling . have to check up */
                    Log.e(TAG,"IOexception in select all");
                    e.printStackTrace();
                    continue;
                }

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
                try {
                    Socket sock = SocketManager.OpenSocket(n.port);
                    SocketManager.WriteToSocket(sock, objMessage);
                    SocketManager.CloseSocket(sock);
                }
                catch (IOException e)
                {
                    /* i dont need to handle failure here as I am handling it while synchronizing */
                    Log.e(TAG,"IO exception during delete");
                    e.printStackTrace();
                }
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
