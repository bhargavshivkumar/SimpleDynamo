package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

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
        DatabaseHelper(Context context){
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
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        Context context = getContext();
        DatabaseHelper dbHelper = new DatabaseHelper(context);
        /**
         * Create a write able database which will trigger its
         * creation if it doesn't already exist.
         */
        db = dbHelper.getWritableDatabase();
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

    public static void writeToDb(String key,String value,String replica)
    {
        Log.d("PROVDER","Inserting into database");
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


    }

    public static Cursor ReadReplicasFromDb(String replica)
    {
        Log.d("PROVIDER","select query from db with replica="+replica);
        return db.rawQuery("SELECT key,value FROM "+TABLE_NAME+" WHERE replica=?",new String[]{replica});
    }

    public static Cursor RepopulateReplicasFromOwnerDbs()
    {

            return db.rawQuery("SELECT key,value FROM "+TABLE_NAME+" WHERE replica is null",null);

    }

    public static Cursor ReadAllFromDb()
    {
      return db.rawQuery("SELECT key,value FROM "+TABLE_NAME,null);
    }

    public static int DeleteFromDB(String key)
    {
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
        return status;
    }


    public static Cursor ReadFromDb(String key)
    {
        //Start query code

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);

            qb.appendWhere("key='"+key+"'");



        Cursor c = qb.query(db,	new String[]{"key","value"},null, null,
                null, null, null);
        //watch uri for change
        //c.setNotificationUri(getContext().getContentResolver(), uri);

        Log.e("Provider", c.getCount()+" rows returned from db");
        return c;
    }

}
