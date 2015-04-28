package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Created by bhargav on 4/21/15.
 */
public class SocketManager {

    public static final String TAG = "Socket Manager";   /*
    Socket helpers to perform socket operations
    */

    public static void CloseSocket(Socket sock) {
        try{
            sock.close();
        }
        catch(IOException e)
        {
            Log.e(TAG, "Socket Close error");
        }
    }

    public static void WriteToSocket(Socket sock,MessageHandler message) {
        try{
            ObjectOutputStream oos =  new ObjectOutputStream(sock.getOutputStream());
            oos.writeObject(message);
        }
        catch(IOException e)
        {
            Log.e(TAG,"Socket Writing error");
            e.printStackTrace();
        }
    }

    public static MessageHandler ReadFromSocket(Socket sock) {
        try{
            ObjectInputStream is =  new ObjectInputStream(sock.getInputStream());
            MessageHandler mess = (MessageHandler)is.readObject();
            return mess;
        }
        catch(Exception e)
        {
            Log.e(TAG,"Socket Reading error");
            e.printStackTrace();
        }
        return null;
    }

    public static String ReceiveAcknowledgement(Socket sock) {
        try{
            ObjectInputStream is =  new ObjectInputStream(sock.getInputStream());
            String mess = (String)is.readObject();
            return mess;
        }
        catch(Exception e)
        {
            Log.e(TAG,"Socket Acknowledgement error");
            e.printStackTrace();
        }
        return null;
    }

    public static Socket OpenSocket(String port) {

        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            return socket;
        }
        catch(IOException e)
        {
            Log.e(TAG,"Socket opening error");
            e.printStackTrace();
        }
        return null;
    }
}
