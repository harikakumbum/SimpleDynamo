/*
References:
 * https://developer.android.com/training/data-storage/sqlite.html
 * https://developer.android.com/reference/android/content/ContentProvider.html
 * https://docs.oracle.com/javase/tutorial/networking/sockets/index.html
 * https://developer.android.com/reference/android/database/MatrixCursor.html
 * https://developer.android.com/reference/java/io/PrintWriter.html
 * https://docs.oracle.com/javase/7/docs/api/java/util/Collections.html
 * https://developer.android.com/reference/org/json/JSONObject.html
 * https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html
*/
package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.ECField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String[] node_list = {"5562","5556","5554","5558","5560"};
    ArrayList<String> hash_list = new ArrayList<String>();
    HashMap<String,String> node_hm = new HashMap<String, String>();
    static DbHelper dynamo_helper;
    static String my_port;
    MatrixCursor global_query_cursor = null;
    boolean global_db_received = false;
    private Semaphore recoveryLock;
    int MSG_TYPE = 0;
    int KEY = 1;
    int VALUE = 2;
    int ORIGIN_PORT = 3;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        String tableName = "dynamoTable";
        String hashedKey = null;
        String key_owner = null;
        try {
            hashedKey = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            key_owner = getKeyOwner(hashedKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(selection.equals("*")) {

            String globalDelReq = "*dbDelete#" + null + "#" + null + "#" + my_port;
            dynamo_helper.getWritableDatabase().delete(tableName,null,null);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, globalDelReq, String.valueOf(my_port));

        } else if (selection.equals("@")) {
            dynamo_helper.getWritableDatabase().delete(tableName,null,null);
        } else {
            if (key_owner.equals(my_port)) {
                String[] selArgs = {selection};
                dynamo_helper.getWritableDatabase().delete(tableName,"key = ?",selArgs);
            } else {
                String query_msg = "deleteReq#" + selection + "#" + null + "#" + String.valueOf(Integer.parseInt(my_port) * 2);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_msg, key_owner);
            }
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        String tableName = "dynamoTable";
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        String hashedKey = null;
        String owner_port = null;
        try {
            hashedKey = genHash(key);
            owner_port = getKeyOwner(hashedKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String insert_msg = "insert#" + key + "#" + value;
        Log.v("insert_msg: ",insert_msg);
        Log.v("dest_port: ",owner_port);
        int index = 0;
        try {
            index = hash_list.indexOf(genHash(String.valueOf(owner_port)));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.v("index: ", String.valueOf(index));
        for(int i = 0; i < 3; i++) {
            String remotePort = node_hm.get(hash_list.get((index + i) % 5));
            Log.v("ports: ",String.valueOf(remotePort));
            Socket cs = null;
            try {
                cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort)*2);
                cs.setSoTimeout(200);
                PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                Log.v("Client: ", "sendingInsertMsg: " + insert_msg);
                Log.v("Client: ", "dest port: " + String.valueOf(remotePort));
                if(i == 0)
                    cpw.println(insert_msg+"#owner");
                else if(i == 1)
                    cpw.println(insert_msg+"#replica1");
                else
                    cpw.println(insert_msg);

            } catch (IOException e) {
                Log.e("error in insert",String.valueOf(e));
                e.printStackTrace();
            }
        }
        return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        dynamo_helper = new DbHelper(getContext());
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        my_port = String.valueOf((Integer.parseInt(portStr)));
        for(int i = 0; i < node_list.length; i++) {
            String node_hash = null;
            try {
                node_hash = genHash(node_list[i]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            hash_list.add(node_hash);
            node_hm.put(node_hash,node_list[i]);
        }
        Collections.sort(hash_list);
        dynamo_helper.getWritableDatabase().delete("dynamoTable",null,null);
        try {
            Log.v(TAG, "Create a ServerSocket");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        recoveryLock = new Semaphore(1, true);
        String recovery_msg = "recovery#" + null + "#" + null + "#" + my_port;
        /*try {
            recoveryLock.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recovery_msg, String.valueOf(my_port));
        //recoveryLock.release();
        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        String tableName = "dynamoTable";
        Cursor cursor = null;
        String hashedKey = null;
        String key_owner = null;
        try {
            hashedKey = genHash(selection);
            key_owner = getKeyOwner(hashedKey);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.v("query method: ",selection);
        //try {
            //recoveryLock.acquire();
            if(selection.equals("*")) {
                global_db_received = false;
                String queryReq = "*dbQuery#" + null + "#" + null + "#" + my_port;
                global_query_cursor = new MatrixCursor(new String[]{"key", "value"});
                String q = "SELECT * FROM dynamoTable";
                Cursor localcursor = dynamo_helper.getReadableDatabase().rawQuery(q, null);
                localcursor.moveToFirst();
                while (!localcursor.isAfterLast()) {
                    Object[] values = {localcursor.getString(0), localcursor.getString(1)};
                    global_query_cursor.addRow(values);
                    localcursor.moveToNext();
                }
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryReq, String.valueOf(my_port));
                while(!global_db_received) {

                }
                cursor = global_query_cursor;
                //recoveryLock.release();
                return cursor;
            } else if (selection.equals("@")) {

                Log.v("Query: ","local dump query");
                String q = "SELECT * FROM dynamoTable";
                cursor = dynamo_helper.getReadableDatabase().rawQuery(q, null);
                if (cursor != null)
                    cursor.moveToFirst();
                //recoveryLock.release();
                return cursor;
            } else {
                int index = 0;
                try {
                    index = hash_list.indexOf(genHash(key_owner));
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                String dest_port = node_hm.get(hash_list.get((index + 2) % 5));
                String dest_port2 = node_hm.get(hash_list.get((index + 1) % 5));
                Log.v("dest port: ",dest_port);
                if (dest_port.equals(my_port)) {
                   cursor = dynamo_helper.getReadableDatabase().query(tableName, new String[]{"key", "value"},
                            "key=?", new String[]{selection}, null, null, null);
                    while(cursor.getCount() <= 0) {
                        cursor = dynamo_helper.getReadableDatabase().query(tableName, new String[]{"key", "value"},
                                "key=?", new String[]{selection}, null, null, null);
                    }
                    //recoveryLock.release();
                    return cursor;
                } else if (dest_port2.equals(my_port)){
                    cursor = dynamo_helper.getReadableDatabase().query(tableName, new String[]{"key", "value"},
                            "key=?", new String[]{selection}, null, null, null);
                    //recoveryLock.release();
                    while(cursor.getCount() <= 0) {
                        cursor = dynamo_helper.getReadableDatabase().query(tableName, new String[]{"key", "value"},
                                "key=?", new String[]{selection}, null, null, null);
                    }
                    return cursor;
                } else {
                    String query_msg = "queryReq#" + selection + "#"  + "#" + String.valueOf(Integer.parseInt(my_port) * 2);
                    String db_res = null;
                    MatrixCursor sel_query_cursor;
                    try {

                        String response = "waiting";
                        while(response.equals("waiting")) {
                            boolean replica_fail = false;
                            try {
                                replica_fail = false;
                                Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(dest_port)*2);
                                cs.setSoTimeout(500);
                                PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                                Log.v("Client: ", "sendingQueryReqMsg: " + query_msg);
                                Log.v("Client: ", "dest port: " + String.valueOf(dest_port));
                                cpw.println(query_msg);
                                BufferedReader query_response = new BufferedReader(new InputStreamReader(cs.getInputStream()));
                                while (true) {
                                    String msg = query_response.readLine();
                                    if (msg != null && !msg.equals("")) {
                                        Log.v("Client : Priority res :", msg);
                                        String[] str_received = msg.split("#");
                                        //return str_received[VALUE];
                                        replica_fail = false;
                                        response = str_received[VALUE];
                                    } else {
                                        replica_fail = true;
                                        break;
                                    }
                                }
                            } catch (IOException e) {
                                replica_fail = true;
                            } catch (Exception e) {
                                replica_fail = true;
                            } finally {
                                if (replica_fail) {
                                    try {
                                        Log.v("replica ", "failed");

                                        Socket cs1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                                Integer.parseInt(dest_port2) * 2);
                                        cs1.setSoTimeout(500);
                                        PrintWriter cpw1 = new PrintWriter(cs1.getOutputStream(), true);
                                        Log.v("Client: ", "sendingQueryReqMsg: " + query_msg);
                                        Log.v("Client: ", "dest port: " + String.valueOf(dest_port2));
                                        cpw1.println(query_msg);
                                        BufferedReader query_response1 = new BufferedReader(new InputStreamReader(cs1.getInputStream()));
                                        while (true) {
                                            String msg1 = query_response1.readLine();
                                            if (msg1 != null) {
                                                Log.v("Client : Priority res :", msg1);
                                                String[] str_received1 = msg1.split("#");
                                                replica_fail = false;
                                                //return str_received1[VALUE];
                                                response = str_received1[VALUE];
                                            } else if (msg1.equals("")) {
                                                break;
                                            }
                                        }
                                    } catch (Exception e) {
                                        continue;
                                    }
                                }
                            }
                            replica_fail = false;
                        }
                        db_res = response;
                        if(db_res != null) {
                            JSONObject jsonObject = new JSONObject(db_res);
                            JSONArray keysArray = jsonObject.getJSONArray("keys");
                            JSONArray valuesArray = jsonObject.getJSONArray("values");
                            sel_query_cursor = new MatrixCursor(new String[]{"key", "value"});
                            for (int p = 0; p < keysArray.length(); p++) {
                                Object[] values = {keysArray.getString(p), valuesArray.getString(p)};
                                sel_query_cursor.addRow(values);
                            }
                            Log.v("query count: ", String.valueOf(sel_query_cursor.getCount()));
                            return sel_query_cursor;
                        }
                        //recoveryLock.release();
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
            }
            //recoveryLock.release();
        /*} catch (InterruptedException e) {
            e.printStackTrace();
        }*/
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
    private String getKeyOwner(String key_hash) throws NoSuchAlgorithmException {
        String key_owner = null;
	    for(int i = 0; i < hash_list.size(); i++) {
            String node_hash = hash_list.get(i);
	        String prev_node_hash;
	        if (i == 0) {
                prev_node_hash = hash_list.get(4);
            } else {
                prev_node_hash = hash_list.get(i-1);
            }
            if(prev_node_hash.compareTo(node_hash) > 0 && key_hash.compareTo(prev_node_hash) > 0) {
	            key_owner = node_hash;
                return node_hm.get(key_owner);
            } else if (prev_node_hash.compareTo(node_hash) > 0 && key_hash.compareTo(node_hash) < 0) {
	            key_owner = node_hash;
                return node_hm.get(key_owner);
            } else if (prev_node_hash.compareTo(key_hash) < 0 && key_hash.compareTo(node_hash) <= 0) {
	            key_owner = node_hash;
                return node_hm.get(key_owner);
            }
        }
	    return node_hm.get(key_owner);
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while(true) {
                    Socket ss1 = serverSocket.accept();
                    //ss1.setSoTimeout(500);
                    DataInputStream dis = new DataInputStream(ss1.getInputStream());
                    String msg = dis.readLine();
                    PrintWriter spw = new PrintWriter(ss1.getOutputStream(), true);

                    if (msg != null) {
                        String[] str_received = msg.trim().split("#");
                        String msg_type = str_received[MSG_TYPE];
                        Log.v("server: ","msg_type: " + msg_type);
                        if(msg_type.equals("insert")) {
                            //recoveryLock.acquire();
                            String key = str_received[KEY];
                            String value = str_received[VALUE];
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
                            ContentValues cVals = new ContentValues();
                            cVals.put("key", key);
                            cVals.put("value", value);
                            Log.v("Server: insert - ", key);
                            if(key != null && key != "null") {
                                if(str_received.length == 4 && str_received[3].equals("owner")) {
                                    msg = "insertreplicas#"+key+"#"+value;
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(my_port));
                                }
                                if(str_received.length == 4 && str_received[3].equals("replica1")) {
                                    msg = "insertreplicas2#"+key+"#"+value;
                                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(my_port));
                                }

                                dynamo_helper.getWritableDatabase().insertWithOnConflict("dynamoTable", null, cVals, SQLiteDatabase.CONFLICT_REPLACE);
                                //dis.close();

                            }
                            //recoveryLock.release();
                        } else if(msg_type.equals("queryReq")) {
                            Log.v("queryReq: ",msg);
                            String key = str_received[KEY];
                            String dest_port = str_received[ORIGIN_PORT];

                            Cursor cursor = query(null,null,key,null,null);

                            //while (cursor.getCount() <= 0)
                            //cursor = query(null,null,key,null,null);
                            Log.v("server: ","cursor length" + cursor.getCount());
                            if(cursor.getCount() > 0) {
                                JSONObject localDB = new JSONObject();
                                JSONArray keyArr = new JSONArray();
                                JSONArray valueArr = new JSONArray();
                                cursor.moveToFirst();
                                for(int j = 0; !cursor.isAfterLast(); j++) {
                                    keyArr.put(j, cursor.getString(cursor.getColumnIndex("key")));
                                    valueArr.put(j, cursor.getString(cursor.getColumnIndex("value")));
                                    cursor.moveToNext();
                                }
                                localDB.put("keys", keyArr);
                                localDB.put("values", valueArr);
                                cursor.close();
                                String dbResponse = "dbResponse#" + key + "#" + localDB.toString();
                                spw.println(dbResponse);
                            } else {
                                spw.println("");
                            }
                        } else if(msg_type.equals("*dbQuery")) {
                            Log.v("Server: ", "dbQuery");
                            int dest_port = Integer.parseInt(str_received[ORIGIN_PORT]);
                            JSONObject localDB = new JSONObject();
                            String q = "SELECT * FROM dynamoTable";
                            Cursor localcursor = dynamo_helper.getReadableDatabase().rawQuery(q, null);
                            JSONArray keyArr = new JSONArray();
                            JSONArray valueArr = new JSONArray();
                            localcursor.moveToFirst();
                            for(int j = 0; !localcursor.isAfterLast(); j++) {
                                keyArr.put(j, localcursor.getString(localcursor.getColumnIndex("key")));
                                valueArr.put(j, localcursor.getString(localcursor.getColumnIndex("value")));
                                localcursor.moveToNext();
                            }
                            localDB.put("keys", keyArr);
                            localDB.put("values", valueArr);
                            String db_response = "*dbResponse#" + null + "#" + localDB.toString() + "#" + my_port;
                            spw.println(db_response);
                            localcursor.close();
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, dbResponse, String.valueOf(dest_port));
                        } else if (msg_type.equals("*dbDelete")) {

                            dynamo_helper.getWritableDatabase().delete("dynamoTable",null,null);

                        } else if(msg_type.equals("deleteReq")) {

                            String selection = str_received[KEY];
                            String[] selArgs = {selection};
                            dynamo_helper.getWritableDatabase().delete("dynamoTable","key = ?",selArgs);

                        } else if(msg_type.equals("recover")) {
                            Log.v("recover: ",str_received[ORIGIN_PORT]);
                            String origin_port = str_received[ORIGIN_PORT];
                            JSONObject localDB = new JSONObject();
                            String q = "SELECT * FROM dynamoTable";
                            Cursor localcursor = dynamo_helper.getReadableDatabase().rawQuery(q, null);
                            if(localcursor.getCount() > 0) {
                                JSONArray keyArr = new JSONArray();
                                JSONArray valueArr = new JSONArray();
                                localcursor.moveToFirst();
                                for(int j = 0; !localcursor.isAfterLast(); j++) {
                                    String key = localcursor.getString(localcursor.getColumnIndex("key"));
                                    String hashed_key = null;
                                    hashed_key = genHash(key);
                                    if(key != null && getKeyOwner(hashed_key).equals(origin_port)) {
                                        keyArr.put(j, key);
                                        valueArr.put(j, localcursor.getString(localcursor.getColumnIndex("value")));
                                    }
                                    localcursor.moveToNext();
                                }
                                localDB.put("keys", keyArr);
                                localDB.put("values", valueArr);
                                String rec_response = "";
                                if(keyArr.length() > 0)
                                    rec_response = "recoveryResponse#" + "#" + localDB.toString() + "#" + my_port;
                                spw.println(rec_response);
                                localcursor.close();

                            } else {

                                spw.println("");
                                localcursor.close();

                            }
                        }
                    }
                }
            } catch (Exception e) {
                Log.e(TAG, "Error in server task");
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            return;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            try {
                String[] msg_received = msgs[0].split("#");
                String dest_port = msgs[1];

                Log.v("client: ","msg_type: " + msg_received[MSG_TYPE]);
                if(msg_received[MSG_TYPE].equals("insert")) {


                } else if(msg_received[MSG_TYPE].equals("insertreplicas")
                        || msg_received[MSG_TYPE].equals("insertreplicas2")) {

                    int index = hash_list.indexOf(genHash(dest_port));
                    Log.v("index: ", String.valueOf(index));
                    int j = 3;
                    if(msg_received[MSG_TYPE].equals("insertreplicas2"))
                        j = 2;
                    for(int i = 1; i < j; i++) {
                        try {
                            String remotePort = node_hm.get(hash_list.get((index + i) % 5));
                            Log.v("ports: ", String.valueOf(remotePort));
                            Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort) * 2);
                            cs.setSoTimeout(200);
                            PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                            Log.v("Client: ", "sendingInsertMsg: " + msgs[0]);
                            Log.v("Client: ", "dest port: " + String.valueOf(remotePort));
                            String msg = "insert#" + msg_received[KEY] + "#" + msg_received[VALUE];
                            cpw.println(msg);
                        } catch (IOException e) {
                            Log.e("Error in insert", String.valueOf(e));
                            continue;
                        }
                    }
                } else if(msg_received[MSG_TYPE].equals("queryReq")) {

                } else if (msg_received[MSG_TYPE].equals("*dbQuery")) {
                    int index = Arrays.asList(node_list).indexOf(dest_port);
                    for(int i = 1; i < 5; i++) {
                        try {
                            int remotePort = 2*Integer.parseInt(node_list[(index + i) % 5]);
                            Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    remotePort);
                            cs.setSoTimeout(200);
                            PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                            Log.v("Client: ", "sending*QueryMsg: " + msgs[0]);
                            Log.v("Client: ", "dest port: " + String.valueOf(remotePort));
                            cpw.println(msgs[0]);
                            BufferedReader query_response = new BufferedReader(new InputStreamReader(cs.getInputStream()));
                            while (true) {
                                String msg = query_response.readLine();

                                Log.v("Client : Priority res :", msg);
                                if (msg != null && !msg.equals("")) {
                                    String[] str_received = msg.split("#");
                                    String db_res = str_received[VALUE];
                                    JSONObject jsonObject = new JSONObject(db_res);
                                    JSONArray keysArray = jsonObject.getJSONArray("keys");
                                    JSONArray valuesArray = jsonObject.getJSONArray("values");
                                    for (int p = 0; p < keysArray.length(); p++) {
                                        Object[] values = {keysArray.getString(p), valuesArray.getString(p)};
                                        global_query_cursor.addRow(values);
                                    }
                                    break;
                                } else {
                                    break;
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                            continue;
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    global_db_received = true;
                } else if(msg_received[MSG_TYPE].equals("*dbDelete")){
                    int index = Arrays.asList(node_list).indexOf(dest_port);
                    for(int i = 1; i < 5; i++) {
                        int remotePort = 2*Integer.parseInt(node_list[(index + i) % 5]);
                        Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                remotePort);
                        cs.setSoTimeout(200);
                        PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                        Log.v("Client: ", "sending*DeleteMsg: " + msgs[0]);
                        Log.v("Client: ", "dest port: " + String.valueOf(remotePort));
                        cpw.println(msgs[0]);
                    }
                } else if(msg_received[MSG_TYPE].equals("deleteReq")) {
                    int index = hash_list.indexOf(genHash(dest_port));
                    Log.v("index: ", String.valueOf(index));
                    for(int i = 0; i < 3; i++) {
                        String remotePort = node_hm.get(hash_list.get((index + i) % 5));
                        Log.v("ports: ",String.valueOf(remotePort));
                        Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort)*2);
                        cs.setSoTimeout(200);
                        PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                        cpw.println(msgs[0]);
                    }
                } else if(msg_received[MSG_TYPE].equals("recovery")) {
                    int index = hash_list.indexOf(genHash(my_port));
                    for(int i = 2; i > -3; i--) {
                        if(i == 0)
                            continue;
                        String remotePort = node_hm.get(hash_list.get((index + 5 + i) % 5));
                        Log.v("ports: ",remotePort);
                        String dest_port3 = my_port;
                        if(i < 0)
                            dest_port3 = remotePort;
                        String msg = "recover#" + null + "#" + null + "#" + dest_port3;;
                        try {
                            Socket cs = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(remotePort) * 2);
                            cs.setSoTimeout(500);
                            PrintWriter cpw = new PrintWriter(cs.getOutputStream(), true);
                            cpw.println(msg);
                            BufferedReader query_response = new BufferedReader(new InputStreamReader(cs.getInputStream()));
                            while (true) {
                                String response = query_response.readLine();
                                if (response != null && response != "") {
                                    String[] str_received = response.split("#");
                                    String db_res = str_received[VALUE];
                                    JSONObject jsonObject = new JSONObject(db_res);
                                    JSONArray keysArray = jsonObject.getJSONArray("keys");
                                    JSONArray valuesArray = jsonObject.getJSONArray("values");
                                    for (int p = 0; p < keysArray.length(); p++) {
                                        //Object[] values = {keysArray.getString(p), valuesArray.getString(p)};
                                        String key = keysArray.getString(p);
                                        String value = valuesArray.getString(p);
                                        //Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");
                                        if ((key != null) && (key != "null") && nodeContainsKey(key)) {
                                            ContentValues cVals = new ContentValues();
                                            cVals.put("key", key);
                                            cVals.put("value", value);
                                            Log.v("Server: insert - ", key);
                                            Cursor cursor = dynamo_helper.getReadableDatabase().query("dynamoTable", new String[]{"key", "value"},
                                                    "key=?", new String[]{key}, null, null, null);
                                            if(cursor.getCount() <= 0)
                                                dynamo_helper.getWritableDatabase().insertWithOnConflict("dynamoTable", null, cVals, SQLiteDatabase.CONFLICT_REPLACE);
                                        }
                                    }
                                    break;
                                } else {
                                    break;
                                }
                            }
                            query_response.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            continue;
                        } catch (Exception e) {
                            continue;
                        }
                    }
                    //recoveryLock.release();
                }
                //recoveryLock.release();
            } catch (Exception e) {
                Log.e("error in client task",String.valueOf(e));
                e.printStackTrace();
            }
            return null;
        }
    }
    private boolean nodeContainsKey (String key) throws NoSuchAlgorithmException {
        int index = hash_list.indexOf(genHash(my_port));
        String begin_port = hash_list.get((index + 5 - 3) % 5);
        String end_port = genHash(my_port);
        String key_hash = genHash(key);
        if(begin_port.compareTo(end_port) < 0) {
            if(begin_port.compareTo(key_hash) < 0 && key_hash.compareTo(end_port) <= 0){
                return true;
            } else {
                return false;
            }
        } else {
            if(begin_port.compareTo(key_hash) < 0 || key_hash.compareTo(end_port) <= 0){
                return true;
            } else {
                return false;
            }
        }

    }
}
