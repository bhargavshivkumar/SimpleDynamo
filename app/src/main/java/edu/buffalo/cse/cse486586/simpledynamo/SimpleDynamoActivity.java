package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.IOException;
import java.net.ServerSocket;

public class SimpleDynamoActivity extends Activity {


    static String myPort;
    static final int SERVER_PORT = 10000;
    public static final String TAG = "Dynamo Activity";
    static SimpleDynamoActivity var;

    static DynamoManager objDynamoManager;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

        var=this;

         /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        objDynamoManager = new DynamoManager();

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            objDynamoManager.new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e)
        {
            Log.e(TAG,"Cant Create Server Socket");
        }

        objDynamoManager.InitializeNode(myPort);


        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        //Set test button event handler
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}

    public void WriteToUI(final String str)
    {
        //reference -http://www.intertech.com/Blog/android-non-ui-to-ui-thread-communications-part-1-of-5/#ixzz3W29JbKDq
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                TextView tv = (TextView) findViewById(R.id.textView1);
                tv.append(str+"\n");
            }
        });


    }
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
