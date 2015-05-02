package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.SynchronousQueue;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {


    public static SQLiteDatabase db;


    static final String URL = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
    static final Uri CONTENT_URI = Uri.parse(URL);
    static final String DATABASE_NAME = "Dynamo.db";
    static final String TABLE_NAME = "data";
    static final int DATABASE_VERSION = 1;
    static final String CREATE_DB_TABLE =
            " CREATE TABLE " + TABLE_NAME +
                    " (" +
                    " key TEXT NOT NULL, " +
                    " value TEXT NOT NULL," +
                    " replica TEXT," +
                    "  UNIQUE(key) ON CONFLICT REPLACE);";


    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        int reVal=0;

        reVal=SimpleDynamoActivity.objDynamoManager.HandleDelete(selection);



        return reVal;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        String key=values.getAsString("key");
        String value = values.getAsString("value");

        Log.e("Insert",key+";"+value);



        SimpleDynamoActivity.objDynamoManager.HandleInsert(key,value);



        Log.e("insert",""+uri);
        return uri;
	}

    /*
   Helper class is used to manage the SQLlite connections
    */
    private static class DatabaseHelper extends SQLiteOpenHelper {

        private static DatabaseHelper instance = null;

        private DatabaseHelper(Context context){
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase db)
        {
            db.execSQL("DROP TABLE IF EXISTS " +  TABLE_NAME);
            db.execSQL(CREATE_DB_TABLE);

        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion,
                              int newVersion) {
            db.execSQL("DROP TABLE IF EXISTS " +  TABLE_NAME);
            onCreate(db);
        }

        public static DatabaseHelper getInstance(Context ctx) {
            /**
             * use the application context as suggested by CommonsWare.
             * this will ensure that you dont accidentally leak an Activitys
             * context (see this article for more information:
             * http://android-developers.blogspot.nl/2009/01/avoiding-memory-leaks.html)
             */
            if (instance == null) {
                instance = new DatabaseHelper(ctx.getApplicationContext());
            }
            return instance;
        }


    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        Context context = getContext();
       // DatabaseHelper dbHelper = new DatabaseHelper(context);
        /**
         * Create a write able database which will trigger its
         * creation if it doesn't already exist.
         */
      //  db = dbHelper.getWritableDatabase();

        db= DatabaseHelper.getInstance(context).getWritableDatabase();
        return (db == null)? false:true;

	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        Cursor cursor;
        Log.e("Provider",selection+" : "+ selection.equals("*"));



        if(selection.equals("*") || selection.equals("\"*\""))
        {
            cursor =SimpleDynamoActivity.objDynamoManager.HandleSelectAll(selection);
        }
        else if(selection.equals("@") || selection.equals("\"@\""))
        {
            cursor =SimpleDynamoActivity.objDynamoManager.HandleSelectAt(selection);
        }
        else {

            cursor = SimpleDynamoActivity.objDynamoManager.HandleQuery(selection);

        }




        return cursor;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static void writeToDb(String key,String value,String replica,boolean isBoot)
    {

        try {
            if(!isBoot)
                DynamoManager.SynchSem.acquire();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
        Log.d("PROVIDER","Inserting into database");
        Log.d("PROVIDER","Key: "+key+" Value: "+value+" replica: "+replica);
        //SimpleDynamoActivity.var.WriteToUI("Inserting Replica: "+replica);
        ContentValues values = new ContentValues();
        values.put("key",key);
        values.put("value",value);
        values.put("replica",replica);
        //start insertion to SQL lite
        long id= db.insert(TABLE_NAME,"",values);

        //check if insertion was not successful
        if(!(id >0))
        {
            Log.e("Provider","Key not inserted in Db");
        }
        if(!isBoot)
        DynamoManager.SynchSem.release();


    }

    public static Cursor ReadReplicasFromDb(String replica,boolean isBoot)
    {
        try {
            if(!isBoot)
            DynamoManager.SynchSem.acquire();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
        Log.d("PROVIDER","select query from db with replica="+replica);

        if(!isBoot)
        DynamoManager.SynchSem.release();

        return db.rawQuery("SELECT key,value FROM "+TABLE_NAME+" WHERE replica=?",new String[]{replica});

    }

    public static Cursor RepopulateReplicasFromOwnerDbs(boolean isBoot)
    {
        try {
            if(!isBoot)
                DynamoManager.SynchSem.acquire();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }

            Cursor cur= db.rawQuery("SELECT key,value FROM "+TABLE_NAME+" WHERE replica is null",null);

            if(!isBoot)
                DynamoManager.SynchSem.release();

        return cur;

    }

    public static Cursor ReadAllFromDb(boolean isBoot)
    {
        try {
            if(!isBoot)
                DynamoManager.SynchSem.acquire();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
      Cursor cur= db.rawQuery("SELECT key,value FROM "+TABLE_NAME,null);
        if(!isBoot)
            DynamoManager.SynchSem.release();

        return cur;
    }

    public static int DeleteFromDB(String key,boolean isBoot)
    {
        try {
            if(!isBoot)
                DynamoManager.SynchSem.acquire();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }

        int status;
        if(key.equals("@") ||key.equals("\"@\"") || key.equals("*") || key.equals("\"*\""))
        {
            //delete all rows
            Log.d("Provider","Deleting all rows");
            status = db.delete(TABLE_NAME,null,null);

        }
        else
        {
            Log.d("Provider","Deleting key: "+key);
            status=db.delete(TABLE_NAME,"key=?",new String[]{key});

        }

        if(!isBoot)
        {
            DynamoManager.SynchSem.release();
        }
        return status;
    }


    public static Cursor ReadFromDb(String key,boolean isBoot)
    {
        //Start query code
        try {
            if(!isBoot)
                DynamoManager.SynchSem.acquire();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);

            qb.appendWhere("key='"+key+"'");



        Cursor c = qb.query(db,	new String[]{"key","value"},null, null,
                null, null, null);
        //watch uri for change
        //c.setNotificationUri(getContext().getContentResolver(), uri);

        Log.e("Provider", c.getCount()+" rows returned from db");

        if(!isBoot)
            DynamoManager.SynchSem.release();

        return c;
    }

}
