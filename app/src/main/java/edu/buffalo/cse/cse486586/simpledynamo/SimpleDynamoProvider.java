package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Exchanger;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    int recoveryCounter = 0;
    public SQLiteHelper databaseHelper;
    SQLiteDatabase database;

    public static final String DATABASE_NAME = "SimpleDynamo";
    public static final int DATABASE_VERSION = 1;
    public static final String TABLE_NAME = "messages";

    Object recoverLock = new Object();
    public Integer[] nodes = {11108, 11116, 11120, 11124, 11112};
    public String[] nodeHashes = {"33d6357cfaaf0f72991b0ecd8c56da066613c089",
            "abf0fd8db03e5ecb199a9b82929e9db79b909643", "c25ddd596aa7c81fa12378fa725f706d54325d12",
            "177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "208f7f72b198dadd244e61801abe1ec3a4857bc9"};

    int myPort;
    static final int SERVER_PORT = 10000;

    ConcurrentHashMap<String, String> queryResults = new ConcurrentHashMap();
    ConcurrentHashMap<String, String> buffer = new ConcurrentHashMap();


    private static boolean recoveryMode = false;

    private static ConcurrentHashMap<String, Object> locks =
            new ConcurrentHashMap<String, Object>();

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            Message message = msgs[0];

            // abstracted, so that thread doesn't know type of message :)

            Log.v("outgoing", "Sending " + message.getType() + " to " + message.getDestination() + " ( port " + message.getPort() + " )");

            try {
                int port = Integer.parseInt(message.getDestination());
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                Log.v(TAG, "Socket to " + String.valueOf(port) + " created");


                ObjectOutputStream outgoingStream = new ObjectOutputStream(socket.getOutputStream());

                try {
                    outgoingStream.writeObject(message);
                } catch (Exception e) {
                    Log.v(TAG, "Ignoring EOF");

                }

                outgoingStream.flush();
                outgoingStream.close();
                socket.close();

                Log.v("outgoing", "Sent " + message.getType() + " to " + message.getDestination() + " ( port " + message.getPort() + " )");


            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }


    }


    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.v("ServerTask", "Started server");


            while (true) {
                try {
                    Socket incomingSocket = null;
                    ObjectInputStream incomingStream = null;

                    incomingSocket = serverSocket.accept();


                    incomingStream = new ObjectInputStream(incomingSocket.getInputStream());


                    Message message = null;


                    message = (Message) incomingStream.readObject();


                    Log.v("incoming", " Received a message of type " + message.getType() + " from " + String.valueOf(message.getPort()));

                    incomingSocket.close();


                    String type = message.getType();


                    if (type.equals("insert")) {



                        if (recoveryMode) { // Push all inserts into temp buffer

                            String key = message.getKey();
                            String value = message.getValue();
                            buffer.put(key,value);
                            continue;
                        }

                        String key = message.getKey();
                        String value = message.getValue();
                        ContentValues values = new ContentValues();

                        values.put("key", key);
                        values.put("value", value);

                        Log.v("INSERT_", "Inserting " + key + " \n" + value);

                        database = databaseHelper.getWritableDatabase();
                        database.replace(TABLE_NAME, null, values);


                        Object object = locks.get(key);
                        queryResults.put(key, value);

                        if (object != null) {
                            locks.remove(key);
                            synchronized (object) {
                                Log.v("NOTIFY", key + "\n" + value);
                                object.notifyAll();
                            }
                        }


                    } else if (type.equals("delete")) {

                        String key = message.getKey();

                        if (key.equals("*")) {
                            databaseHelper.getWritableDatabase().rawQuery("DELETE * from messages", null);
                        } else {
                            String[] sArgs = {key};
                            databaseHelper.getReadableDatabase().delete("messages", "key = ?", sArgs);
                        }

                    } else if (type.equals("query") || type.equals("query_recover")) {


                        String key = message.getKey();
                        String requestingNode = message.getPort();

                        if (key.equals("*")) {
                            Log.v("QUERY", "Sending * to " + requestingNode);

                            Cursor cursor = databaseHelper.getReadableDatabase().rawQuery("SELECT * from messages", null);
                            Message result = new Message("result_recover");

                            if (type.equals("query")) {
                                result = new Message("result");
                            }

                            result.setKey("*");
                            result.setDestination(requestingNode);
                            result.setPort(String.valueOf(myPort));

                            cursor.moveToPosition(-1);
                            result.body = "";
                            while (cursor.moveToNext()) {

                                result.body += cursor.getString(0) + "," + cursor.getString(1) + ";";
                            }
                            cursor.close();
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, result);


                        }


                        if (!key.equals("*")) { // Querying local DB

                            if (recoveryMode) {
                                continue;
                            }
                            Log.v("QUERY", "Querying local partition for query request for key " + key + " from " + requestingNode);

                            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                            queryBuilder.setTables(TABLE_NAME);

                            String value = null;
                            Cursor cursor = queryBuilder.query(databaseHelper.getReadableDatabase(),
                                    null, "key=?", new String[]{key}, null, null, null);
                            if (cursor.moveToFirst()) {
                                value = cursor.getString(cursor.getColumnIndex("value"));
                            }

                            if (value == null) {
                                Log.v("QUERY_", "value not found. waiting");
                                Object object = locks.get(key);
                                if (object == null) {
                                    object = new Object();
                                    locks.put(key, object);
                                }

                                synchronized (object) {
                                    try {
                                        Log.v("QUERY_", "WAITING FOR " + key);
                                        object.wait();
                                        Log.v("QUERY_", "READY");

                                    } catch (InterruptedException e) {
                                        Log.e(TAG, "Key wait interrupted");
                                    }
                                }
                                cursor = queryBuilder.query(databaseHelper.getReadableDatabase(),
                                        null, "key=?", new String[]{key}, null, null, null);
                            }

                            if (cursor.moveToFirst()) {
                                value = cursor.getString(cursor.getColumnIndex("value"));
                            }

                            cursor.close();


                            Message result = new Message("result");
                            result.setDestination(requestingNode);
                            result.setPort(String.valueOf(myPort));
                            result.setKey(key);
                            result.setValue(value);

                            Log.v("QUERY_RESULT", "key " + key + " value " + value);


                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, result);


                        }

                    } else if (type.equals("result") || type.equals("result_recover")) {
                        String key = message.getKey();
                        String value = message.getValue();

                        if (key.equals("*")) {
                            Log.v("QUERY_ALL_REPLY", "Received query all reply from " + message.getPort());
                            Log.v("QUERY_ALL_REPLY", "Received data " + message.getBody());

                            StringBuilder messages = new StringBuilder(message.getBody());
                            if (messages.length() != 0) {

                                messages.deleteCharAt(message.getBody().length() - 1);
                                String[] allMessages = messages.toString().split(";");

                                for (String msg : allMessages) {
                                    String[] msgParts = msg.split(",");
                                    key = msgParts[0];
                                    value = msgParts[1];
                                    Log.v("QUERY_ALL_REPLY", "key " + key + " value " + value);
                                    if (type.equals("result_recover")) {

                                        boolean decision = amIResponsible(key);
                                        if (!decision) {
                                            Log.v("INSERT_", "Skipping " + key + " \n" + value);

                                            continue;
                                        }
                                        ContentValues values = new ContentValues();

                                        values.put("key", key);
                                        values.put("value", value);

                                        Log.v("INSERT_", "Inserting " + key + " \n" + value);

                                        database = databaseHelper.getWritableDatabase();
                                        database.replace(TABLE_NAME, null, values);


                                    } else {
                                        queryResults.put(key, value);

                                    }


                                }
                            }


                            if (type.equals("result_recover")) {
                                recoveryCounter--;
                                if (recoveryCounter <= 0) {
                                    // Push data from buffer into database
                                    for(String key_: buffer.keySet()) {
                                        String value_ = buffer.get(key_);

                                        ContentValues values = new ContentValues();

                                        values.put("key", key_);
                                        values.put("value", value_);

                                        Log.v("INSERT_", "Inserting " + key_ + " \n" + value_);

                                        database = databaseHelper.getWritableDatabase();
                                        database.replace(TABLE_NAME, null, values);
                                    }
                                    buffer.clear();
                                    recoveryMode = false;
                                    Log.v("RECOVER", "Rejoin complete");
                                    synchronized (recoverLock) {
                                        try {
                                            recoverLock.notifyAll();
                                        } catch (Exception e) {

                                        }
                                    }

                                }
                            }

                        } else {
                            queryResults.put(key, value);

                            synchronized (queryResults) {
                                queryResults.notify();
                            }
                            Log.v("QUERY_RESULT", "Recevied result from " + message.getPort() + " key " + key + " value " + value);

                        }


                    }

                } catch (Exception e) {
                    Log.v("Exception", "Exception in ServerTask");
                    e.printStackTrace();

                }
            }

            //Log.v("FATAL", "Stopped listening for sockets ?");
            //return null;
        }

        protected void onProgressUpdate(Message... messages) {
            return;
        }


    }


    public int findHome(String key) {
        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {

        }
        Log.v("findHome", key);
        Log.v("findHome", keyHash);

        for (int i = 0; i < nodes.length; i++) {
            int pred = i;
            int succ = (i + 1) % nodes.length;

            if (nodeHashes[pred].compareTo(nodeHashes[succ]) > 0) {
                if (keyHash.compareTo(nodeHashes[pred]) > 0 || keyHash.compareTo(nodeHashes[succ]) <= 0) {

                    return succ;
                }

            } else if (keyHash.compareTo(nodeHashes[pred]) > 0 && keyHash.compareTo(nodeHashes[succ]) <= 0)
                return succ;
        }

        return -1;

    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String key = selection;


        if (key.equals("*")) {
            databaseHelper.getWritableDatabase().rawQuery("DELETE * from messages", null);

            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] == myPort)
                    continue;
                try {

                    Message message = new Message("delete");
                    message.setKey(key);
                    message.setDestination(String.valueOf(nodes[i]));
                    message.setPort(String.valueOf(myPort));


                    int port = Integer.parseInt(message.getDestination());
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);

                    ObjectOutputStream outgoingStream = new ObjectOutputStream(socket.getOutputStream());
                    outgoingStream.writeObject(message);
                    outgoingStream.flush();
                    outgoingStream.close();
                    socket.close();

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            return 0;
        } else if (key.equals("@")) {
            databaseHelper.getWritableDatabase().rawQuery("DELETE * from messages", null);
            return 0;
        }

        int firstCoord = findHome(key);
        int secondCoord = (firstCoord + 1) % nodes.length;
        int thirdCoord = (firstCoord + 2) % nodes.length;


        Message message = new Message("delete");
        message.setKey(key);
        message.setDestination(String.valueOf(nodes[firstCoord]));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

        Message message2 = new Message("delete");
        message2.setKey(key);
        message2.setDestination(String.valueOf(nodes[secondCoord]));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2);

        Message message3 = new Message("delete");
        message3.setKey(key);
        message3.setDestination(String.valueOf(nodes[secondCoord]));
        message3.setDestination(String.valueOf(nodes[thirdCoord]));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message3);


        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        if (recoveryMode) {
            synchronized (recoverLock) {
                try {
                    Log.v("RECOVER_WAIT", "waiting");
                    recoverLock.wait();
                    Log.v("RECOVER_WAIT", "done waiting");

                } catch (Exception e) {
                    e.printStackTrace();

                }
            }
        }

        String key = values.getAsString("key");
        String value = values.getAsString("value");

        int firstCoord = findHome(key);
        int secondCoord = (firstCoord + 1) % nodes.length;
        int thirdCoord = (firstCoord + 2) % nodes.length;

        Log.v("INSERT_", "First co-ordinator is " + nodes[firstCoord]);
        Log.v("INSERT_", "Second co-ordinator is " + nodes[secondCoord]);
        Log.v("INSERT_", "Third co-ordinator is " + nodes[thirdCoord]);


        Message message = new Message("insert");
        message.setKey(key);
        message.setValue(value);
        message.setDestination(String.valueOf(nodes[firstCoord]));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

        Message message2 = new Message("insert");
        message2.setKey(key);
        message2.setValue(value);
        message2.setDestination(String.valueOf(nodes[secondCoord]));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2);

        Message message3 = new Message("insert");
        message3.setKey(key);
        message3.setValue(value);
        message3.setDestination(String.valueOf(nodes[secondCoord]));
        message3.setDestination(String.valueOf(nodes[thirdCoord]));
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message3);


        return uri;


    }


    @Override
    public boolean onCreate() {
        databaseHelper = new SQLiteHelper(getContext(), DATABASE_NAME, DATABASE_VERSION);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE); // need getContext() to use getSystemService
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr)) * 2;


        SharedPreferences crash_recovery = this.getContext().getSharedPreferences("crash_recovery", 0);
        if (crash_recovery.getBoolean("start", true)) {
            crash_recovery.edit().putBoolean("start", false).commit();
        } else {

            recoveryMode = true;
        }


        // Server Task to listen to incoming messages.
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            //return false;
        }


        // Recover from failure
        if (recoveryMode) {
            recoveryCounter = 4;
            Log.v("RECOVER", "Just recovered from crash!");
            databaseHelper.getWritableDatabase().execSQL("DELETE FROM messages"); /* Need to block query till my messages table
                                                                                      is filled again.*/
            for (int i = 0; i < nodes.length; i++) {

                if (nodes[i] == myPort)
                    continue;
                Log.v("RECOVER", "Sending recover query to " + nodes[i]);

                Message message = new Message("query_recover");
                message.setKey("*");
                message.setPort(String.valueOf(myPort));
                message.setDestination(String.valueOf(nodes[i]));

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);


            }

            // Force out of recoveryMode if I don't switch back within T seconds ?


        }


        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {


        if (recoveryMode) {
            synchronized (recoverLock) {
                try {
                    Log.v("RECOVER_WAIT", "waiting");
                    recoverLock.wait();
                    Log.v("RECOVER_WAIT", "done waiting");

                } catch (Exception e) {
                    e.printStackTrace();

                }
            }
        }

        synchronized (this) {


            String key = selection;


            if (key.equals("*")) {
                Log.v("QUERY", "Returning data from all partitions");

                Cursor cursor = databaseHelper.getReadableDatabase().rawQuery("SELECT * from messages", null);

                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] == myPort)
                        continue;
                    try {

                        Message message = new Message("query");
                        message.setKey(key);
                        Log.v("QUERY", "Sending " + key + " to " + String.valueOf(nodes[i]));
                        message.setDestination(String.valueOf(nodes[i]));
                        message.setPort(String.valueOf(myPort));


                        int port = Integer.parseInt(message.getDestination());
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);

                        ObjectOutputStream outgoingStream = new ObjectOutputStream(socket.getOutputStream());
                        outgoingStream.writeObject(message);
                        outgoingStream.flush();
                        outgoingStream.close();
                        socket.close();

                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                        e.printStackTrace();
                    }
                }
                // Add key,values from queryResults to cursor.

                Log.v("QUERY", "Sleeping");

                // Using Hashtable here kills the process for some reason.

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();

                }

                Log.v("QUERY", "Waking up");

                String[] fields = {"key", "value"};
                MatrixCursor newCursor = new MatrixCursor(fields);
                for (String k : queryResults.keySet()) {
                    String v = queryResults.get(k);
                    String[] row = {k, v};

                    newCursor.addRow(row);
                }

                // From stack overflow
                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{newCursor, cursor});


                return mergeCursor;

            }

            if (key.equals("@")) {
                Log.v("QUERY", "Returning data from local partition " + recoveryMode);
                return databaseHelper.getReadableDatabase().rawQuery("SELECT * from messages", null);
            }

                /*Normal Q */
            locks.clear();

            int firstCoord = findHome(key);
            int secondCoord = (firstCoord + 1) % nodes.length;
            int thirdCoord = (firstCoord + 2) % nodes.length;

            {
                Log.v("QUERY_F", "key " + key);


                Message message = new Message("query");
                message.setKey(key);
                message.setDestination(String.valueOf(nodes[thirdCoord]));
                message.setPort(String.valueOf(myPort));
                Log.v("QUERY_F", "Forwarding key " + key + " to" + nodes[thirdCoord]);

                Timer timer = new Timer();
                timer.schedule(new Interruptor(nodes[firstCoord], nodes[secondCoord], nodes[thirdCoord], message), 2000);


                Timer timer2 = new Timer();
                timer.schedule(new Interruptor(nodes[secondCoord], nodes[firstCoord], nodes[thirdCoord], message), 4000);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);


                synchronized (queryResults) {
                    try {


                        Log.v("QUERY_F", "WAITING FOR " + selection);
                        queryResults.wait();
                        if (queryResults.get(key) == null) {
                            Log.v("FIXING", "Exception about to happen");
                            Log.v("FIXING", queryResults.toString());
                            queryResults.wait(); // Handle incorrect failure detection.

                            //new RuntimeException("Value is null");

                        }

                        timer.cancel();
                        timer.purge();
                        timer2.cancel();
                        timer2.purge();
                        //Log.v("FIXING", queryResults.toString());


                        Log.v("QUERY_F", "READY");
                    } catch (InterruptedException e) {

                        Log.e(TAG, "QUERY_F interrupted");
                    }
                }


                String[] fields = {"key", "value"};
                MatrixCursor cursor = new MatrixCursor(fields);
                String[] values = {key, queryResults.get(key)};
                cursor.addRow(values);

                Log.v("QUERY_F", key + " \n" + queryResults.get(key));
                return cursor;


            }


        }
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

    protected class Interruptor extends TimerTask {
        int firstCoord, secondCoord, thirdCoord;
        Message query;

        Interruptor(int firstCoord, int secondCoord, int thirdCoord, Message query) {
            this.firstCoord = firstCoord;
            this.secondCoord = secondCoord;
            this.thirdCoord = thirdCoord;
            this.query = query;
        }

        public void run() {
            Log.v("INTERRUPTOR", "Node" + thirdCoord + "failed");

            query.setDestination(String.valueOf(secondCoord));

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query);

        }
    }

    private boolean amIResponsible(String key) {

        int firstCoord = findHome(key);
        int secondCoord = (firstCoord + 1) % nodes.length;
        int thirdCoord = (firstCoord + 2) % nodes.length;


        if (myPort == nodes[firstCoord] || myPort == nodes[secondCoord] || myPort == nodes[thirdCoord])
            return true;
        else

            return false;
    }


}
