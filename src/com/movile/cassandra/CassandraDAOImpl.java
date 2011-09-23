package com.movile.cassandra;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 * Generic dao operations
 * 
 * check the Hector source code for more details (it doesnt have documentation):
 * @see https://github.com/rantav/hector/tree/master/core/src/main/java/me/prettyprint/hector/api
 * @see https://github.com/rantav/hector/wiki/Getting-started-%285-minutes%29
 * 
 * cassandra tutorial by datastax:
 * @see https://github.com/zznate/cassandra-tutorial
 */
public class CassandraDAOImpl extends CassandraBase {

    private String columnFamily;
    /**
     * Internal Enum
     */
    public static enum Type {
        STRING,
        LONG,
        BYTE_ARRAY
    };

    // default constructor
    public CassandraDAOImpl(String columnFamily) {
        super();
        this.columnFamily = columnFamily;
    }

    /**
     * Deletes a key and all of the related columns
     * @param id column key
     * @throws HectorException
     */
    public void delete(String id) throws HectorException {
        deleteColumn(id, null);
    }

    /**
     * Deletes a column related to a key
     * @param id key
     * @param column key related
     * @throws HectorException
     */
    public void deleteColumn(String id, String column) throws HectorException {
        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        mutator.delete(id, columnFamily, column, stringSerializer);
    }

    /**
     * Updates a specific column inside key
     * @param id key
     * @param column key of column
     * @param value value to be changed
     * @param type enum describing the type of data
     * @throws HectorException
     */
    public void update(final String id, String column, Object value, Type type) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily, stringSerializer,
                stringSerializer);

        ColumnFamilyUpdater<String, String> updater = template.createUpdater(id);
        updater.setDate(column, new Date());
        
        if (type.equals(Type.STRING)) {
            updater.setString(column, (String) value);
        } else if (type.equals(Type.LONG)) {
            updater.setLong(column, (Long) value);
        } else if (type.equals(Type.BYTE_ARRAY)) {
            updater.setByteArray(column, (byte[]) value);
        } else {
            throw new InvalidParameterException("Invalid type");
        }

        template.update(updater);
    }
    
    /**
     * Updates a specific column inside key, and return the new timestamp of the column
     * @param id key
     * @param column key of column
     * @param value value to be changed
     * @param type enum describing the type of data
     * @throws HectorException
     */
    public Long updateColumn(final String id, String columnKey, Object value, Type type) throws HectorException {

        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        String COLUMN_FAMILY = columnFamily;
        Long timestamp = 0L;

        if (type.equals(Type.STRING)) {
            HColumn<String, String> column = HFactory.createStringColumn(columnKey, (String) value);
            mutator.insert(id, COLUMN_FAMILY, column);
            timestamp = column.getClock();
            
        } else if (type.equals(Type.LONG)) {
            HColumn<String, Long> column = HFactory.createColumn(columnKey, (Long) value, stringSerializer, longSerializer);
            mutator.insert(id, COLUMN_FAMILY, column);
            timestamp = column.getClock();
            
        } else if (type.equals(Type.BYTE_ARRAY)) {
            HColumn<String, byte[]> column = HFactory.createColumn(columnKey, (byte[]) value, stringSerializer, byteArraySerializer);
            mutator.insert(id, COLUMN_FAMILY, column);
            timestamp = column.getClock();
            
        } else {
            throw new InvalidParameterException("Invalid type");
        }

        return timestamp/1000L; // return in ms
    }

    
    /**
     * Get all of String columns from a column family key
     * @param id key of column family
     * @return a Map with related parameters
     * @throws HectorException
     */
    public Map<String,String> getColumns(final String id) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily, stringSerializer,
                stringSerializer);

        ColumnFamilyResult<String, String> res = template.queryColumns(id);
        
        Map<String,String> columns = new TreeMap<String, String>();
        
        if (res.hasResults()) {
            
               Collection<String> columnList = res.getColumnNames();
               
               for (String columnName : columnList) {
                   HColumn<String, ByteBuffer> column = res.getColumn(columnName);
                   String value = stringSerializer.fromByteBuffer(column.getValue());
                   columns.put(column.getName(), value);
               }
        }

        return columns;
    }
    
    

    /**
     * Retrieve data from a specific column
     * @param id key
     * @param column key of related column
     * @param type enum describing the type of data
     * @return the retrieved data of null otherwise
     * @throws HectorException
     */
    public Object getColumnValue(final String id, String column, Type type) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily, stringSerializer,
                stringSerializer);

        ColumnFamilyResult<String, String> res = template.queryColumns(id);

        if (type.equals(Type.STRING)) {
            return res.getString(column);
        } else if (type.equals(Type.LONG)) {
            return res.getLong(column);
        } else if (type.equals(Type.BYTE_ARRAY)) {
            return res.getByteArray(column);
        } else {
            return null;
        }
    }

    /**
     * Get a cassandra's column
     * @param id
     * @param columnkey
     * @return
     * @throws HectorException
     */
    public HColumn<String, ByteBuffer> getColumn(final String id, String columnkey) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily, stringSerializer,
                stringSerializer);

        HColumn<String, ByteBuffer> column = null;
        
        ColumnFamilyResult<String, String> res = template.queryColumns(id);
        if (res.hasResults()) {
            column = res.getColumn(columnkey);
        }
        
        return column;
    }
}
