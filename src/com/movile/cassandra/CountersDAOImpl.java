package com.movile.cassandra;

import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 * Counter operations v0.8 feature
 */
public class CountersDAOImpl extends CassandraBase {

    private String columnFamily;

    // default constructor
    public CountersDAOImpl(String columnFamily) {
        super();
        this.columnFamily = columnFamily;
    }

    /**
     * Increments a counter
     * @param key the column key
     * @param columnName the column name
     */
    public void increment(String key, String columnName) {
        add(key, columnName, 1L);
    }

    /**
     * Decrements a counter
     * @param key the column key
     * @param columnName the column name
     */
    public void decrement(String key, String columnName) {
        add(key, columnName, -1L);
    }

    /**
     * 
     * @param key
     * @param columnName
     * @return
     */
    public long get(String key, String columnName) {
        
        CounterQuery<String, String> query = HFactory.createCounterColumnQuery(keyspace, stringSerializer, stringSerializer);
        query.setKey(key);
        query.setName(columnName);
        query.setColumnFamily(columnFamily);
        
        QueryResult<HCounterColumn<String>> result = query.execute();
        return result.get().getValue();
        
    }
    
    /**
     * Add a value and changes the counter value (Counter Column Family)
     * @param key the column key
     * @param columnName column name
     * @param value positive values to add and negative values to decrement the counter
     */
    public void add(String key, String columnName, long value) {

        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        HCounterColumn<String> column = HFactory.createCounterColumn(columnName, value);

        mutator.addCounter(key, columnFamily, column);
        mutator.execute();
    }
}
