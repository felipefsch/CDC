package logBased;

import java.io.DataInput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import org.apache.cassandra.db.CounterUpdateColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

import utils.Utils;

public class LogDeserializer {

	public final static int INSERTION_MASK		= 0; // changed from original 0x01 to the number indeed
    public final static int DELETION_MASK       = 1; // because of the way I deserialized the data
    public final static int EXPIRATION_MASK     = 2; // otherwise the comparisons won't work
    public final static int COUNTER_MASK        = 4;
    public final static int COUNTER_UPDATE_MASK = 8;
    
    // Necessary because I couldn't get the metadata of the column family by its ID
    public static List<Integer> standardColumn = new ArrayList<Integer>();
    public static List<Integer> superColumn = new ArrayList<Integer>();
//    public static HashMap<Integer, String> colFamNames = new HashMap<Integer, String>();
    
//    public static String superColFamName = "Salaries";
//    public static String standardColFamName = "Grades";
	
    public static List<LogRow> deserialize(DataInput dis) throws Exception
    {   
//    	standardColumn.add(1022);
//    	superColumn.add(1021);
//    	colFamNames.put(1021, superColFamName);
//    	colFamNames.put(1022, standardColFamName);
//
//    	superColumn.add(1033);
//    	colFamNames.put(1033, "Performance");
//    	
    	superColumn.add(1037);
    	superColumn.add(1046);
//    	colFamNames.put(1037, "Performance");
    	
    	LogRow keyValueAux = new LogRow();    	
    	LogRow keyValue = new LogRow();
    	
        String keyspace = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
        
        // Number of modifications
        int size = dis.readInt();
        keyValueAux.setNumberModifications(size);
        List<LogRow> lr = new ArrayList<LogRow>();
        for (int i = 0; i < size; ++i)
        {        	
        	if (!keyspace.equals("system") && !keyspace.equals("local"))
            {
        		String strKey = Utils.toString(key);
        		System.out.println("KS: " + keyspace + " key: " + strKey + " size: " + size);
        		
	            keyValueAux.setKeyspace(keyspace);	            
	            keyValueAux.setKey(strKey);	            
                Integer cfid = Integer.valueOf(dis.readInt());
                keyValueAux.setColumnFamilyId(cfid);                
                
	            lr = deserialize(dis, keyValueAux);
            }
        }                

        return lr;
    }
    
    public static List<LogRow> deserialize(DataInput dis, LogRow keyValue) throws Exception
    {
    	boolean notEmptyColumn = dis.readBoolean();
    	keyValue.setBool(notEmptyColumn);
    	
        if (!keyValue.isBool()){
        	return null;
        }           

        // create a ColumnFamily based on the cf id
        // I need the ColumnFamily information to distinguish between standard column and super column. It's important when I deserialize the row mutations
        int cfId = dis.readInt();        
        keyValue.setColumnFamilyId(cfId);
//        System.out.println(keyValue.getColumnFamilyId());
//        keyValue.setColumnFamily(colFamNames.get(cfId));        
        keyValue.setColumnFamily(Integer.toString(cfId));
        
        // Take the metadata for the CfId
//        try {
//        	CFMetaData cfm = Schema.instance.getCFMetaData(cfId);
//        }
//        catch (Exception e) {
//        	System.out.println("noCfName");
//        }
        
        int localDeletionTime = dis.readInt();
        keyValue.setLocalDeletionTime(localDeletionTime);
        
        long marketForDeletionAt = dis.readLong();
        keyValue.setMarketForDeleteAt(marketForDeletionAt);
        
        int numColumns = dis.readInt();
        
        // key was deleted
        List<LogRow> lr = new ArrayList<LogRow>();
        if (numColumns == 0) {
        	keyValue.setOperation(LogRow.MutationType.DELETION);
        	keyValue.setTimestamp(marketForDeletionAt);
        	lr.add(keyValue);
        	return lr;
        }
        //For now, we are just using standard columns!! No need of this check!
        //if (standardColumn.contains(keyValue.getColumnFamilyId()))        
        for (int i = 1; i <= numColumns; i++) {
        	LogRow l = new LogRow(keyValue);
			deserializeStandardColumn(dis, l);
			lr.add(l);
        }
       // else if ((superColumn.contains(keyValue.getColumnFamilyId())))
       // 	deserializeSuperColumn(dis, keyValue, numColumns);
//        else 
//        	return keyValue;
        return lr;
    }
    
    public static void deserializeStandardColumn(DataInput dis, LogRow keyValue) throws Exception {

	    	ByteBuffer columnName = ByteBufferUtil.readWithShortLength(dis);
	        keyValue.setColumnName(toString(columnName));        
	        
	        int serializationFlag = dis.readUnsignedByte();
	        if ((serializationFlag & COUNTER_MASK) != 0)
	        {
	            long timestampOfLastDelete = dis.readLong();
	            System.out.print("[" + timestampOfLastDelete + "]");
	            long ts = dis.readLong();
	            System.out.print("[" + ts + "]");
	            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
	            System.out.print("[" + value + "]");
	            System.out.print("COUNTER_MASK");
	            keyValue = null;
	        }
	        else if ((serializationFlag & EXPIRATION_MASK) != 0)
	        {
	            int ttl = dis.readInt();
	            System.out.print("[" + ttl + "]");
	            int expiration = dis.readInt();
	            System.out.print("[" + expiration + "]");
	            long ts = dis.readLong();
	            System.out.print("[" + ts + "]");
	            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
	            System.out.print("[" + value + "]");
	            System.out.print("EXPIRATION_MASK");
	            keyValue = null;
	        }
	        else if (serializationFlag == DELETION_MASK) {
	            long ts = dis.readLong();
	            keyValue.setTimestamp(ts);
	            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
	            keyValue.setValue(toString(value));
	        	keyValue.setOperation(LogRow.MutationType.DELETION);
	        }
	        else if (serializationFlag == INSERTION_MASK) {
	            long ts = dis.readLong();
	            keyValue.setTimestamp(ts);
	            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
	            keyValue.setValue(toString(value));
	        	keyValue.setOperation(LogRow.MutationType.INSERTION);
	        }
	        // after here is probably useless
	        else
	        {
	            long ts = dis.readLong();
	            keyValue.setTimestamp(ts);
	            System.out.print("[" + keyValue.getTimestamp() + "]");
	            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
	            keyValue.setValue(toString(value));
	            System.out.print("[" + keyValue.getValue() + "]");
	            if ((serializationFlag & COUNTER_UPDATE_MASK) != 0) {
	            	System.out.print("COUNTER_UPDATE_MASK");
	            	keyValue.setOperation(LogRow.MutationType.COUNTER_UPDATE);
	            	new CounterUpdateColumn(columnName, value, ts);
	            }
	            else
	            {
	            	System.out.print("NO_MASK");
	            	keyValue.setOperation(LogRow.MutationType.INSERTION);
	            }
	        }

    }
    
    public static void deserializeSuperColumn(DataInput dis, LogRow keyValue, int numColumns) throws Exception {
    	 ByteBuffer superColumnName = ByteBufferUtil.readWithShortLength(dis);    	
    	 keyValue.setSuperColumnName(toString(superColumnName));    	 
    	 
         int localDeletionTime = dis.readInt();
         
         if (localDeletionTime != Integer.MIN_VALUE && localDeletionTime <= 0)
         {
             throw new IOException("Invalid localDeleteTime read: " + localDeletionTime);
         }
         long markedForDeleteAt = dis.readLong();
         
         // Deleted SuperColumn
         if (markedForDeleteAt > 0) {
        	 keyValue.setOperation(LogRow.MutationType.DELETION);
        	 keyValue.setSuperColumnMarketForDeleteAt(markedForDeleteAt);
        	 keyValue.setSuperColumnLocalDeletionTime(localDeletionTime);
        	 // Set the timestamp with the ts of deletion. Easier to handle afterwards.
        	 keyValue.setTimestamp(markedForDeleteAt);
         }

         /* read the number of columns */
         int size = dis.readInt();
         keyValue.setNumberStandardColumns(size);
         
         // Just if it has sub columns
         if (size != 0) {
        	 /**
        	  * TODO: SEVERAL STANDARD COLUMNS SERIALIZED!!!
        	  */
        	 deserializeStandardColumn(dis, keyValue);
         }
    }

    public static String toString(ByteBuffer buffer) 
    throws UnsupportedEncodingException
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }
    
	
}
