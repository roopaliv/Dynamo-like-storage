package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;


import java.io.Serializable;


class Data implements Serializable {
	int id;
	String task;
	String sentByPort;
	String receivedByPort;
	Set<DataItem> dataItems;
	List<String> ports;
	DataItem dataItem;
	long count;
	int sequenceID;
}

class DataItem implements Serializable{
	int version;
	String key;
	String value;
	int owner;
}

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = "SimpleDynamo";
	static String selfPort;
	final int SERVER_PORT = 10000;
	static final int PORT1 = 5554;//11108;
	static final int PORT2 = 5556;//11112;
	static final int PORT3 = 5558;//11116;
	static final int PORT4 = 5560;//11120;
	static final int PORT5 = 5562;//11124;
	ArrayList<String> ALL_PORTS = new ArrayList<String>() {{
		add(Integer.toString(PORT1));
		add(Integer.toString(PORT2));
		add(Integer.toString(PORT3));
		add(Integer.toString(PORT4));
		add(Integer.toString(PORT5));
	}};

	ConcurrentLinkedQueue<Data> sendList = new ConcurrentLinkedQueue<Data>();
	TreeMap<String, String> ring = new TreeMap<String, String>();
	AtomicInteger currentID = new AtomicInteger();
	Lock atom = new ReentrantLock();
	boolean coordinated;
	ConcurrentMap<Integer, ConcurrentLinkedQueue<Data>> simul = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Data>>();
	private SQLiteDatabase dbInstance;
	AtomicInteger currentRequest = new AtomicInteger(0);


	Thread clientThread;
	Thread serverThread;
	Thread coordinationThread;
	PriorityBlockingQueue<Data> msgList = new PriorityBlockingQueue<Data>(90,  new Comparator<Data>(){
		@Override
		public int compare(Data data1, Data data2) {
			int retVal = 0;
			if(coordinated)
				retVal = (data1.sequenceID == data2.sequenceID) ? 0: ((data1.sequenceID > data2.sequenceID ? 1 : -1) );
			else {
				boolean b = data1 == null || data2 == null;
				if(b)
                    retVal = data2 == null ? -1 : 1;
                else {
					boolean coordinate = data1.task.equals("Coordinate");
					boolean coorBack = data1.task.equals("CoorBack");
					boolean b1 = coordinate || coorBack;
					if (b1)
                        retVal = -1;
                    else {
						boolean coordinate1 = data2.task.equals("Coordinate");
						boolean coorBack1 = data2.task.equals("CoorBack");
						boolean b2 = coordinate1 || coorBack1;
						if(b2)
                            retVal = 1;
					}
				}
			}
			return retVal;
		}
	});
	ConcurrentMap<Integer, CountDownLatch> simulMap = new ConcurrentHashMap<Integer, CountDownLatch>();
	private static class DynamoOpenHelper extends SQLiteOpenHelper {

		DynamoOpenHelper(Context context){
			super(context, "DynamoDb", null, 9);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL("CREATE TABLE DynamoTable ( key TEXT PRIMARY KEY NOT NULL, value TEXT, node TEXT NOT NULL, version INT NOT NULL);");
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS DynamoTable");
			onCreate(db);
		}
	}

	@Override
	public boolean onCreate() {
		try {
			addPorts();
			getMyPort();
			getDb();
			createServerSocket();
			startThreads();
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}

		return true;
	}

	private void startThreads() {
		try {
			clientThread = new clientThread();
			serverThread = new serverThread();
			coordinationThread = new coordinationThread();
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		try {
			clientThread.start();
			serverThread.start();
			coordinationThread.start();
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private void createServerSocket() {
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);;
		} catch (Exception e) {
			Log.e(TAG, "Error: ", e);
		}
	}

	private void getDb() {
		try {
			DynamoOpenHelper dynamoOpenHelper = new DynamoOpenHelper(getContext());
			dbInstance = dynamoOpenHelper.getWritableDatabase();
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private void getMyPort() {
		try{
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			selfPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		}catch(NullPointerException e){
			Log.e(TAG, "Error: ", e );
		}
	}

	private void addPorts() {
		for(String p: ALL_PORTS){
			try {
				ring.put(genHash(p), p);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}

	private class coordinationThread extends Thread{
		@Override
		public void run() {
			Data msg = getData();
			setMsg(msg);
			try {
				Map<String, String> keyValues = new HashMap<String, String>();
				Map<String, Integer> keyVersions = new HashMap<String, Integer>();
				int id = msg.id;
				ConcurrentLinkedQueue<Data> messages = simul.get(id);
				rem(msg);
				ContentValues values = getContentValues(keyValues, keyVersions, messages);
				actualCoord(keyValues, keyVersions, values);
				setCoord();
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
		private void actualCoord(Map<String, String> keyValues, Map<String, Integer> keyVersions, ContentValues values) {
			try {
				atom.lock();
				dbInstance.beginTransaction();
				add(keyValues, keyVersions, values);
			} catch (Exception e) {
				Log.e(TAG, "Error: ", e);
			} finally {
				dbInstance.setTransactionSuccessful();
				dbInstance.endTransaction();
				atom.unlock();
			}
		}
		private void setMsg(Data msg) {
			try{
				int i = 0;
				String[] strings = getStrings(i);
				scheduleToSend(msg, strings);
				waitCoord(msg);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
		private void setCoord() {
			coordinated = true;
		}


		private ContentValues getContentValues(Map<String, String> vals, Map<String, Integer> map, ConcurrentLinkedQueue<Data> messages) {
			coord(vals, map, messages);
			try {
				Cursor cursor = atQuery();
				Set<DataItem> dataItemSet = itemsGet(cursor);
				setCoordRows(vals, map, dataItemSet);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
			ContentValues contentValues = new ContentValues();
			return contentValues;
		}

		private void add(Map<String, String> keyValues, Map<String, Integer> map, ContentValues values) {
			try {
				Set<Map.Entry<String, String>> entries = keyValues.entrySet();
				for (Map.Entry<String, String> entry : entries) {
					try {
						add(map, values, entry);
						addToDb(values);
					} catch (Exception e) {
						Log.e(TAG, "Error: " + e.getMessage());
					}
				}
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void add(Map<String, Integer> map, ContentValues values, Map.Entry<String, String> entry) {
			String key1 = entry.getKey();
			values.put("key", key1);
			String value2 = entry.getValue();
			values.put("value", value2);
			Integer value1 = map.get(key1);
			values.put("version", value1);
			try {
				String key = key1;
				String hash = genHash(key);
				String s = getOwnerHere(hash);
				String value = ring.get(s);
				values.put("node", value);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void addToDb(ContentValues values) {
			try {
				dbInstance.replace("DynamoTable", null, values);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void setCoordRows(Map<String, String> map1, Map<String, Integer> map, Set<DataItem> dataItems) {
			for(DataItem dataItem: dataItems){
				String key = dataItem.key;
				boolean b = map.containsKey(key);
				try {
					if (b)
						existsHere(map1, map, dataItem, key);
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}

		private void existsHere(Map<String, String> vals, Map<String, Integer> map, DataItem dataItem, String key) {
			Integer integer = map.get(key);
			int version = dataItem.version;
			boolean b = integer < version;
			if (b) {
				add(vals, map, dataItem);
			}
		}

		private void add(Map<String, String> vals, Map<String, Integer> map, DataItem dataItem) {
			try {
				String key = dataItem.key;
				int version = dataItem.version;
				map.put(key, version);
				String value = dataItem.value;
				vals.put(key, value);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void coord(Map<String, String> vals, Map<String, Integer> map, ConcurrentLinkedQueue<Data> datas) {
			for(Data message : datas) {
				boolean b = checkPoint(message);
				if (!b) {
					Set<DataItem> data = message.dataItems;
					for (DataItem dataItem : data) {
						String key = dataItem.key;
						try {
							doCoord(vals, map, dataItem, key);
						} catch (Exception e) {
							Log.e(TAG, "Error: " + e.getMessage());
						}
					}
				}
			}
		}

		private boolean checkPoint(Data message) {
			boolean b1 = message == null;
			if (b1 || message.dataItems == null)
				return true;
			return false;
		}

		private void doCoord(Map<String, String> vals, Map<String, Integer> map, DataItem dataItem, String key) {
			boolean b = map.containsKey(key);
			if (!b)
				add(vals, map, dataItem);
			else
				existingKey(vals, map, dataItem, key);
		}

		private void existingKey(Map<String, String> vals, Map<String, Integer> map, DataItem dataItem, String key) {
			int version = dataItem.version;
			Integer integer = map.get(key);
			boolean b = integer < version;
			if(b){
				add(vals, map, dataItem);
			}
		}

		private void rem(Data msg) {
			try {
				int id = msg.id;
				simulMap.remove(id);
				simul.remove(id);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void waitCoord(Data msg) {
			try {
				wait(msg);
			} catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void wait(Data msg) throws InterruptedException {
			try {
				TimeUnit milliseconds = TimeUnit.MILLISECONDS;
				int id = msg.id;
				CountDownLatch countDownLatch = simulMap.get(id);
				countDownLatch.await(4000, milliseconds);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private String[] getStrings(int i) {
			int i1 = ALL_PORTS.size() - 1;
			String[] ports = new String[i1];
			for(String port: ALL_PORTS){
				try {
					i = otherPort(i, ports, port);
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
			return ports;
		}

		private int otherPort(int i, String[] strings, String s) {
			boolean equals = selfPort.equals(s);
			if(!equals){
				try {
					strings[i++] = s;
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
			return i;
		}

		private Data getData() {
			List<String> myNodes;
			Data data;
			try {
				myNodes = listOdNodes(selfPort);
				int id = currentID.incrementAndGet();
				String coordinate = "Coordinate";
				data = new Data();
				data.id = id;
				data.sentByPort = selfPort;
				data.task = coordinate;
				data.receivedByPort = null;
				data.dataItems = null;
				data.ports = null;
				data.dataItem = null;
				data.count = 0;
				data.sequenceID = Integer.MAX_VALUE;
				data.ports = myNodes;
			}catch (Exception e) {
				data = null;
				Log.e(TAG,"Error: "+e.getMessage());
			}
			return data;
		}
	}




	private class clientThread extends Thread{

		@Override
		public void run() {
			while (true){
				try {
					isThere();
					caterOne();
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}

		private void isThere() {
			while (!coordinated) {
				caterAll();
			}
		}

		private void caterOne() {
			boolean empty = msgList.isEmpty();
			if(!empty){
				try {
					Data poll = msgList.poll();
					rcvdData(poll);
				} catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}

		private void caterAll() {
			boolean empty = msgList.isEmpty();
			try {
				if (!empty && isCoordination()) {
					Data poll = msgList.poll();
					rcvdData(poll);
				}
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private boolean isCoordination() {
			Data peek = msgList.peek();
			String task = peek.task;
			boolean b = task.equals("Coordinate") || task.equals("CoorBack");
			return b;
		}


		private void rcvdData(Data data){
			String task = data.task;
			boolean empty = empty(data);
			if (!empty) {
				if (task.equals("Coordinate")) {
					List<String> nodesForPort;
					Cursor cursor;
					try {
						nodesForPort = data.ports;
						cursor = atQuery();
						data.dataItems = null;
						data.ports = null;
						data.dataItem = null;
						data.count = 0;
						Set<DataItem> dt = new HashSet<DataItem>();
						coordAndAdd(data, nodesForPort, cursor, dt);
					} catch (Exception e) {
						Log.e(TAG, "Error: " + e.getMessage());
					}
				} else if (task.equals("QueryGlobal")) {
					try {
						Cursor cursor = atQuery();
						prAndAddQ(data, cursor);
					} catch (Exception e) {
						Log.e(TAG, "Error: " + e.getMessage());
					}

				} else if (task.equals("Query")) {
					Cursor results;
					try {
						results = getRecord(data);

						data.dataItems = null;
						data.ports = null;
						data.dataItem = null;
						data.count = 0;
						setRecord(data, results);
						prepQuery(data);
						sendList.add(data);
					} catch (Exception e) {
						results = null;
						Log.e(TAG, "Error: " + e.getMessage());
					}
				} else if (task.equals("Insert")) {
					try {
						DataItem dataItem = data.dataItem;
						String key = dataItem.key;
						String value = dataItem.value;
						prepAndAddInsert(data, key, value);
					} catch (Exception e) {
						Log.e(TAG, "Error: " + e.getMessage());
					}
				} else if (task.equals("Delete")) {
					DataItem dataItem;
					int count;
					try {
						dataItem = data.dataItem;
						String key = dataItem.key;
						count = localQuery(key).getCount();

						data.dataItems = null;
						data.ports = null;
						data.dataItem = null;
						data.count = 0;
						data.count = count;
						String hash = genHash(key);
						String owner = getOwnerHere(hash);
						String node = ring.get(owner);
						atInsert(key, null, node);
						prepAndAddDel(data);
					} catch (Exception e) {
						Log.e(TAG, "Error: " + e.getMessage());
					}

				} else if (isAnAnswer(task)) {
					try {
						listCon(data);
						list(data);
					} catch (Exception e) {
						Log.e(TAG, "Error: " + e.getMessage());
					}
				}
			}
		}

		private boolean isAnAnswer(String task) {
			boolean coorBack = task.equals("CoorBack");
			boolean globalBack = task.equals("GlobalBack");
			boolean insertBack = task.equals("InsertBack");
			boolean deleteBack = task.equals("DeleteBack");
			boolean queryBack = task.equals("QueryBack");
			boolean b = coorBack || globalBack
					|| insertBack || deleteBack || queryBack;
			return b;
		}



		private boolean empty(Data data) {
			boolean b = data == null;
			if(b)
				return true;
			return false;
		}



		private void prAndAddQ(Data data, Cursor cursor) {
			try {
				prepareAllQ(data, cursor);
				sendList.add(data);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void prepareAllQ(Data data, Cursor results) {
			try {

				data.dataItems = null;
				data.ports = null;
				data.dataItem = null;
				data.count = 0;
				data.dataItems = itemsGet(results);
				data.receivedByPort = data.sentByPort;
				data.sentByPort = selfPort;
				String globalBack = "GlobalBack";
				data.task = globalBack;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void prepAndAddInsert(Data data, String key, String value) {
			try {
				prepare(data);
				InsertAddToList(data, key, value);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void prepare(Data message) {
			try {
				message.receivedByPort = message.sentByPort;
				message.sentByPort = selfPort;
				message.task = "InsertBack";
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void InsertAddToList(Data data, String key, String value) {
			String hash = genHash(key);
			String hashNodeForHash = getOwnerHere(hash);
			String node = ring.get(hashNodeForHash);
			try {
				long idId = atInsert(key, value, node);

				data.dataItems = null;
				data.ports = null;
				data.dataItem = null;
				data.count = 0;
				data.count = idId;
				sendList.add(data);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}



		private void list(Data data) {
			int id = data.id;
			boolean b = simulMap.containsKey(id);
			if(b){
				try {
					CountDownLatch countDownLatch = simulMap.get(id);
					countDownLatch.countDown();
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}

		private void listCon(Data data) {
			int id = data.id;
			boolean b = simul.containsKey(id);
			try {
				if (b) {
					ConcurrentLinkedQueue<Data> datas = simul.get(id);
					datas.add(data);
				}
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void prepAndAddDel(Data data) {
			try {
				PrepDelete(data);
				sendList.add(data);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void PrepDelete(Data data) {
			try {
				data.receivedByPort = data.sentByPort;
				data.sentByPort = selfPort;
				data.task = "DeleteBack";
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}


		private Cursor getRecord(Data data) {
			DataItem dataItem;
			String key;
			Cursor cursor;
			try {
				dataItem = data.dataItem;
				key = dataItem.key;
				cursor = localQuery(key);

			}catch (Exception e) {
				cursor = null;
				Log.e(TAG,"Error: "+e.getMessage());
			}
			return cursor;
		}

		private void setRecord(Data data, Cursor results) {
			Set<DataItem> dataItemSetSet = itemsGet(results);
			int size = dataItemSetSet.size();
			boolean b = size > 0;
			if(b){
				try{
					Iterator<DataItem> iterator = dataItemSetSet.iterator();

					data.dataItems = null;
					data.ports = null;
					data.dataItem = null;
					data.count = 0;
					data.dataItem = iterator.next();
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}

		private void prepQuery(Data data) {
			try{
				data.task = "QueryBack";
				data.receivedByPort = data.sentByPort;
				data.sentByPort = selfPort;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}




		private void coordAndAdd(Data data, List<String> list, Cursor cursor, Set<DataItem> dataItems) {
			try {
				coord(list, cursor, dataItems);
				prepCoord(data, dataItems);
				sendList.add(data);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void prepCoord(Data data, Set<DataItem> dataItems) {
			try {

				data.dataItems = null;
				data.ports = null;
				data.dataItem = null;
				data.count = 0;
				data.dataItems = dataItems;
				data.receivedByPort = data.sentByPort;
				data.sentByPort = selfPort;
				String coorBack = "CoorBack";
				data.task = coorBack;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void coord(List<String> list, Cursor cursor, Set<DataItem> data) {
			for(cursor.moveToFirst();!cursor.isAfterLast();cursor.moveToNext()){
				try {
					DataItem dataItem = new DataItem();
					int key = cursor.getColumnIndex("key");
					dataItem.key = cursor.getString(key);
					int value = cursor.getColumnIndex("value");
					dataItem.value = cursor.getString(value);
					int version = cursor.getColumnIndex("version");
					dataItem.version = cursor.getInt(version);
					int node = cursor.getColumnIndex("node");
					String string = cursor.getString(node);
					dataItem.owner = node;
					boolean contains = list.contains(string);
					if (contains)
						data.add(dataItem);
				}catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}
	}



	private class serverThread extends Thread{
		@Override
		public void run() {
			while (true){
				boolean empty = sendList.isEmpty();
				while (!empty) {
					final Data data;
					try {
						data = getDataMsg();
						String receivedByPort = data.receivedByPort;
						whoseMsg(data, receivedByPort);
					}catch (Exception e) {
						Log.e(TAG,"Error: "+e.getMessage());
					}
				}
			}
		}

		private void whoseMsg(Data data, String receivedByPort) {
			try {
				boolean equals = selfPort.equals(receivedByPort);
				if (equals) {
					myMsg(data);
				} else {
					sendMessage(data);
				}
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private Data getDataMsg() {
			Data data;
			try {
				data = sendList.poll();
			}catch (Exception e) {
				data = null;
				Log.e(TAG,"Error: "+e.getMessage());
			}
			return data;
		}

		private void myMsg(Data data) {
			try {
				int i = currentRequest.incrementAndGet();
				data.sequenceID = i;
				msgList.add(data);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}

		private void sendMessage(Data msg){
			try{
				String receivedByPort = msg.receivedByPort;
				int port = Integer.parseInt(receivedByPort) * 2;
				Socket socket = new  Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msg);
				out.flush();
				out.close();
				socket.close();
			}catch (Exception e){
				Log.e(TAG, "Error: " + msg, e);
			}
		}
	}

	public String otherOwner(String s){
		String prev = null;
		Set<String> strings = ring.keySet();
		for(String key: strings){
			try {
				boolean equals = s.equals(prev);
				if (equals) {
					return key;
				}
				prev = key;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
		String first = ring.firstKey();
		return first;
	}

	public synchronized String getOwnerHere(String s){
		String response;
		try {
			response = ring.ceilingKey(s);
			boolean b = response == null;
			if (b) {
				return ring.firstKey();
			}
		}catch (Exception e) {
			response = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return response;
	}

	public synchronized String[] getOwner(String key){
		String[] strings;
		try {
			strings = new String[3];
			strings[0] = getOwnerHere(genHash(key));
			newLesser(strings);
			newUpper(strings);
		}catch (Exception e) {
			strings = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return strings;
	}

	private void newUpper(String[] strings) {
		int length = strings.length;
		for(int i = 0; i < length; i++){
			try {
				String s = strings[i];
				strings[i] = ring.get(s);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}

	private void newLesser(String[] sends) {
		for(int i = 1; i < 3; i++){
			try {
				String s = sends[i - 1];
				sends[i] = otherOwner(s);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}

	private synchronized void scheduleToSend(Data msg, String[] sends){
		addToLists(msg, sends);
		try {
			for (String port : sends)
				getAndAddtoList(msg, port);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private void getAndAddtoList(Data msg, String port) {
		try {
			Data m1 = getData(msg, port);
			sendList.add(m1);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private void addToLists(Data msg, String[] strings) {
		try {
			int count = strings.length - 1;
			int id = msg.id;
			CountDownLatch value = new CountDownLatch(count);
			simulMap.put(id, value);
			ConcurrentLinkedQueue<Data> value1 = new ConcurrentLinkedQueue<Data>();
			simul.put(id, value1);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private Data getData(Data data1, String port) {
		Data data;
		try {
			data = new Data();
			data.id = data1.id;
			data.sentByPort = data1.sentByPort;
			data.task = data1.task;
			data.receivedByPort = data1.receivedByPort;

			data.dataItems = null;
			data.ports = null;
			data.dataItem = null;
			data.count = 0;

			data.dataItems = data1.dataItems;
			data.ports = data1.ports;
			data.dataItem = data1.dataItem;
			data.count = data1.count;

			data.receivedByPort = port;
		} catch (Exception e) {
			data = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return data;
	}

	private Data getDeletData(String key) {
		Data data;
		int id = currentID.incrementAndGet();
		String delete = "Delete";
		data = new Data();
		data.id = id;
		data.sentByPort = selfPort;
		data.task = delete;
		data.receivedByPort = null;
		data.dataItems = null;
		data.ports = null;
		data.dataItem = null;
		data.count = 0;
		data.sequenceID = Integer.MAX_VALUE;
		DataItem dataItem = new DataItem();
		dataItem.key = key;
		dataItem.value = null;
		dataItem.version = 1;
		data.dataItem = dataItem;
		String[] nodeHashForKey = getOwner(key);
		scheduleToSend(data, nodeHashForKey);
		return data;
	}

	private int getId1(Data msg) throws InterruptedException {
		int id1 = msg.id;
		CountDownLatch countDownLatch = simulMap.get(id1);
		countDownLatch.await();
		return id1;
	}


	private synchronized long atInsert(String key, String value, String node){
		ContentValues values = new ContentValues();
		long id;
		try{
			prepVals(key, value, node, values);
			atom.lock();
			getPrev(key, values);
			id = addDb(values);
		}catch (Exception e) {
			id = -1;
			Log.e(TAG,"Error: "+e.getMessage());
		}finally{
			atom.unlock();
		}
		return id;
	}

	private long addDb(ContentValues values) {
		long id;
		try {
			id = dbInstance.replace("DynamoTable", "", values);
		}catch (Exception e) {
			id = -1;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return id;
	}

	private void getPrev(String key, ContentValues values) {
		Cursor cursor = localQuery(key);
		boolean b = cursor.getCount() > 0;
		if(b){
			cursor.moveToFirst();
			try {
				int version1 = cursor.getColumnIndex("version");
				int version = cursor.getInt(version1);
				int i = 1;
				values.put("version", version + i);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}

	private void prepVals(String key, String value, String node, ContentValues values) {
		try {
			int value1 = 1;
			values.put("version", value1);
			values.put("key", key);
			values.put("value", value);
			values.put("node", node);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private synchronized Cursor localQuery(String key){
		Cursor cursor ;
		try{
			atom.lock();
			cursor = getKeyDb(key);
			cursor.moveToFirst();
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}finally {
			atom.unlock();
		}
		return cursor;
	}

	private Cursor getKeyDb(String key) {
		Cursor cursor;
		try {
			cursor = dbInstance.query("DynamoTable", null, "key" + "=?", new String[]{key}, null, null, null);
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return cursor;
	}

	private Data preInsert(String key, String value) {
		Data data;
		try {
			String[] sendList = getOwner(key);
			int id = currentID.incrementAndGet();
			String insert = "Insert";
			data = new Data();
			data.id = id;
			data.sentByPort = selfPort;
			data.task = insert;
			data.receivedByPort = null;
			data.dataItems = null;
			data.ports = null;
			data.dataItem = null;
			data.count = 0;
			data.sequenceID = Integer.MAX_VALUE;
			DataItem dataItem = new DataItem();
			dataItem.key = key;
			dataItem.value = value;
			dataItem.version = 1;
			data.dataItem = dataItem;

			scheduleToSend(data, sendList);
		}catch (Exception e) {
			data = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return data;
	}

	private Cursor normalQuery(String key) {
		MatrixCursor cursor;
		try{
			Data msg = prepNormalQuery(key);
			String value;
			value = checkNormalQuery(msg, Integer.MIN_VALUE, null);
			cursor = getCurNormal(msg);
			boolean b = value != null;
			if(b){
				cursor.addRow(new Object[]{key, value});
			}
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return cursor;
	}

	private MatrixCursor getCurNormal(Data data) {
		MatrixCursor cursor;
		try{
			modifyList(data.id);
			String[] columnNames = {"key", "value"};
			cursor = new MatrixCursor(columnNames);
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return cursor;
	}

	private Data prepNormalQuery(String key) {
		Data msg;
		try{
			msg = getNormalDt(key);
			int id = msg.id;
			CountDownLatch countDownLatch = simulMap.get(id);
			countDownLatch.await();
		} catch (InterruptedException e) {
			msg = null;
			Log.e(TAG,"Error: ",e);
		}
		return msg;
	}

	private Data getNormalDt(String key) {
		Data data;
		try {
			int id1 = currentID.incrementAndGet();
			String query = "Query";
			data = new Data();
			data.id = id1;
			data.sentByPort = selfPort;
			data.task = query;
			data.receivedByPort = null;
			data.dataItems = null;
			data.ports = null;
			data.dataItem = null;
			data.count = 0;
			data.sequenceID = Integer.MAX_VALUE;
			DataItem dataItem = new DataItem();
			dataItem.key = key;
			dataItem.value = null;
			dataItem.version = 1;
			data.dataItem = dataItem;
			scheduleToSend(data, getOwner(key));
		}catch (Exception e) {
			data = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return data;
	}


	private String[] getStrings(String port) {
		String[] portNodes;
		try {
			portNodes = new String[3];
			String hash = genHash(port);
			portNodes[0] = getOwnerHere(hash);
		}catch (Exception e) {
			portNodes = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return portNodes;
	}

	private void n2(String[] strings) {
		int length = strings.length;
		for(int i = 0; i < length; i++){
			try {
				String portNode = strings[i];
				strings[i] = ring.get(portNode);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}

	private void n1(String[] strings) {
		for(int i = 1; i < 3; i++){
			try {
				String portNode = strings[i - 1];
				strings[i] = lastOwner(portNode);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}


	private String lastOwner(String s){
		String object = ring.firstKey();
		boolean equals = s.equals(object);
		if(equals){
			try{
				String circ = circ();
				return circ;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}else{
			try{
				String s1 = notCirc(s);
				boolean b = s1 != null;
				if (b) return s1;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
		return null;
	}

	private String circ() {
		String s = ring.lastKey();
		return s;
	}

	private String notCirc(String s) {
		String s1 = null;
		Set<String> strings = ring.keySet();
		for(String key: strings){
			try{
				boolean equals = key.equals(s);
				if(equals){
					return s1;
				}
				s1 = key;
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
		return null;
	}


	private class ServerTask extends AsyncTask<ServerSocket, Data, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while(true){
				try {
					Socket socket =  serverSocket.accept();
					InputStream ipStr = socket.getInputStream();
					ObjectInputStream ip  = new ObjectInputStream(ipStr);
					Data msgReceived = (Data) ip.readObject();
					getMsg(msgReceived);
					ip.close();
					socket.close();
				}  catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}

		private void getMsg(Data data) {
			try{
				int i = currentRequest.incrementAndGet();
				data.sequenceID = i;
				msgList.add(data);
			} catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}


	private Cursor atQuery(){
		Cursor cursor;
		try{
			atom.lock();
			cursor = getAllDb();
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}finally{
			atom.unlock();
		}
		return cursor;
	}

	private Cursor getAllDb() {
		Cursor cursor;
		try {
			cursor = dbInstance.query("DynamoTable", null, null, null, null, null, null);
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return cursor;
	}



	private List<String> listOdNodes(String s){
		List<String> strings;
		try {
			String[] portNodes = getStrings(s);
			n1(portNodes);
			n2(portNodes);
			strings = Arrays.asList(portNodes);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
			strings = null;
		}
		return strings;
	}



	private void modifyList(int i) {
		try {
			simul.remove(i);
			simulMap.remove(i);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private Cursor atGet(){
		Cursor cursor;
		try{
			atom.lock();
			String dynamoTable = "DynamoTable";
			String sql = "SELECT key,value from " + dynamoTable + " WHERE value IS NOT NULL";
			cursor = dbInstance.rawQuery(sql, null);
		} catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}finally {
			atom.unlock();
		}
		return cursor;
	}

	private Set<DataItem> itemsGet(Cursor cursor){
		Set<DataItem> values = new HashSet<DataItem>();
		try {
			for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext())
				getAndAdd(cursor, values);
		} catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return values;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		try {
			if (selection.equals("@") || selection.equals("\"@\""))
				return atGet();
			else if (selection.equals("*") || selection.equals("\"*\""))
				return startGet();
			else
				return normalQuery(selection);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return null;
	}


	private void getAndAdd(Cursor result, Set<DataItem> vals) {
		DataItem dataItem;
		try {
			dataItem = getRow(result);
			vals.add(dataItem);
		} catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}


	private Cursor startGet(){
		Data msg = getAndAddQ();
		int id = msg.id;
		try{
			CountDownLatch countDownLatch = simulMap.get(id);
			countDownLatch.await();
		}  catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return getResults(msg.id);
	}



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Data msg;
		int i = 0;
		try{
			msg = getDeletData(selection);
			int id1 = getId1(msg);
			modifyList(id1);
		} catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return i;
	}




	private Data getAndAddQ() {
		Data msg;
		try {
			msg = get();
			addhere(msg);
		} catch (Exception e) {
			msg = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return msg;
	}



	private void addhere(Data d) {
		int size = ALL_PORTS.size();
		try {
			String[] arr = new String[size];
			arr = ALL_PORTS.toArray(arr);
			scheduleToSend(d, arr);
		} catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private Data get() {
		Data data;
		try {
			int id = currentID.incrementAndGet();
			String queryGlobal = "QueryGlobal";
			data = new Data();
			data.id = id;
			data.sentByPort = selfPort;
			data.task = queryGlobal;
			data.receivedByPort = null;
			data.dataItems = null;
			data.ports = null;
			data.dataItem = null;
			data.count = 0;
			data.sequenceID = Integer.MAX_VALUE;
		} catch (Exception e) {
			data = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return data;
	}



	private synchronized Cursor getResults(int i){
		Map<String, Integer> map = new HashMap<String, Integer>();
		Map<String , String> vals = new HashMap<String, String>();
		MatrixCursor cursor;
		try {
			cursor = checkAndGet(i, map, vals);
			modifyList(i);
		}catch (Exception e) {
			cursor = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return cursor;
	}

	private MatrixCursor checkAndGet(int messageId, Map<String, Integer> map, Map<String, String> vals) {
		MatrixCursor results;
		try {
			check(messageId, map, vals);
			results = getMatrixCursor(vals);
		}catch (Exception e) {
			results = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return results;
	}

	private void check(int messageId, Map<String, Integer> map, Map<String, String> vals) {
		ConcurrentLinkedQueue<Data> datas = simul.get(messageId);
		try {
			for (Data message : datas) {
				if (!(message == null || message.dataItems == null)) {
					Set<DataItem> dataItems = message.dataItems;
					getKeys(map, vals, dataItems);
				}
			}
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private MatrixCursor getMatrixCursor(Map<String, String> vals) {
		String[] columnNames = {"key", "value"};
		MatrixCursor cursor = new MatrixCursor(columnNames);
		try{
			Set<Map.Entry<String, String>> entries = vals.entrySet();
			for(Map.Entry<String, String> entry: entries){
				try{
					boolean b = entry.getValue() != null;
					if(b)
						task(cursor, entry);
				} catch (Exception e) {
					Log.e(TAG,"Error: "+e.getMessage());
				}
			}
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return cursor;
	}

	private void task(MatrixCursor result, Map.Entry<String, String> entry) {
		String key = entry.getKey();
		String value = entry.getValue();
		Object[] columnValues = {key, value};
		result.addRow(columnValues);
	}


	private DataItem getRow(Cursor result) {
		DataItem dataItem1;
		try {
			int key = result.getColumnIndex("key");
			DataItem dataItem = new DataItem();
			dataItem.key = result.getString(key);
			dataItem.value = null;
			dataItem.version = 1;
			dataItem1 = dataItem;
			int version = result.getColumnIndex("version");
			dataItem1.version = result.getInt(version);
			int value = result.getColumnIndex("value");
			dataItem1.value = result.getString(value);
		} catch (Exception e) {
			dataItem1 = null;
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return dataItem1;
	}




	private void getKeys(Map<String, Integer> map, Map<String, String> vals, Set<DataItem> dataItems) {
		for(DataItem dataItem: dataItems){
			try {
				String key = dataItem.key;
				boolean b = map.containsKey(key);
				if (b)
					oldKey(map, vals, dataItem);
				else
					newKey(map, vals, dataItem);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}

	private void newKey(Map<String, Integer> map, Map<String, String> vals, DataItem item) {
		String key = item.key;
		String value = item.value;
		try {
			vals.put(key, value);
			int version = item.version;
			map.put(key, version);
		}catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
	}

	private void oldKey(Map<String, Integer> map, Map<String, String> vals, DataItem item) {
		int version = item.version;
		String key = item.key;
		Integer integer = map.get(key);
		boolean b = version > integer;
		if(b){
			try {
				String value = item.value;
				vals.put(key, value);
				map.put(key, version);
			}catch (Exception e) {
				Log.e(TAG,"Error: "+e.getMessage());
			}
		}
	}



	private String checkNormalQuery(Data msg, int version, String value) {
		for(Data data : simul.get(msg.id)){
			boolean b = data == null;
			if(!(b || data.dataItem == null) ) {

				DataItem item = data.dataItem;
				String value1 = item.value;
				try {
					if (value1 != null) {
						int version1 = item.version;
						if (version1 > version) {
							value = value1;
							version = version1;
						}
					}
				} catch (Exception e) {
					Log.e(TAG, "Error: " + e.getMessage());
				}
			}
		}
		return value;
	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		int i = 1;
		Data msg = preInsert(key, value);
		try{
			int id = msg.id;
			CountDownLatch countDownLatch = simulMap.get(id);
			countDownLatch.await();
			modifyList(msg.id);
		} catch (Exception e) {
			Log.e(TAG,"Error: "+e.getMessage());
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	private synchronized String genHash(String input){
		try{
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			byte[] sha1Hash = sha1.digest(input.getBytes());
			Formatter formatter = new Formatter();
			for (byte b : sha1Hash) {
				formatter.format("%02x", b);
			}
			return formatter.toString();
		}catch( Exception e){
			Log.e(TAG,"Exception: ", e);
		}
		return "";
	}
}
