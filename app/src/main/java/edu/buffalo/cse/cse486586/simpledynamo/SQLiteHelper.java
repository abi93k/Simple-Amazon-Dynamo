package edu.buffalo.cse.cse486586.simpledynamo;


import android.util.Log;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;


/**
 * Created by Abhishek on 1/24/16.
 */
public class SQLiteHelper extends SQLiteOpenHelper {


    public SQLiteHelper(Context context, String DATABASE_NAME, int DATABASE_VERSION) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase database) {
        database.execSQL("CREATE TABLE " + edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.TABLE_NAME + " ('key' TEXT PRIMARY KEY, 'value' TEXT)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        /* Not required for this project */

    }
}
