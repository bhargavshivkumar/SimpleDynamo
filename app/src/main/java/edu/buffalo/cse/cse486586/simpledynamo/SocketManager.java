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

    public static void CloseSocket(Socket sock) throws IOException{

            sock.setSoTimeout(5000);
            sock.close();


    }

    public static void WriteToSocket(Socket sock,MessageHandler message) throws IOException {

            sock.setSoTimeout(5000);
            ObjectOutputStream oos =  new ObjectOutputStream(sock.getOutputStream());
            oos.writeObject(message);

    }

    public static MessageHandler ReadFromSocket(Socket sock) throws IOException {
        try{
            sock.setSoTimeout(5000);
            ObjectInputStream is =  new ObjectInputStream(sock.getInputStream());
            MessageHandler mess = (MessageHandler)is.readObject();

            //flushing due to EOF exceptions

            return mess;
        }
        catch(ClassNotFoundException e)
        {
            Log.e(TAG,"Socket Reading error:ClassNotFound");
            e.printStackTrace();
        }
        return null;
    }

    public static String ReceiveAcknowledgement(Socket sock) throws IOException{
        try{
            sock.setSoTimeout(5000);
            ObjectInputStream is =  new ObjectInputStream(sock.getInputStream());
            String mess = (String)is.readObject();
            return mess;
        }
        catch(ClassNotFoundException e)
        {
            Log.e(TAG,"Socket Acknowledgement error:Class not found");
            e.printStackTrace();
        }
        return null;
    }

    public static Socket OpenSocket(String port)  throws IOException {


            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));

            socket.setSoTimeout(5000);

            return socket;

    }
}
