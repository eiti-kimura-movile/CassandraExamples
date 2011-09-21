package com.movile.cassandra;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.Date;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import com.movile.bean.Person;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 * 
 * check the Hector source code for more details (it doesnt have documentation):
 * @see https://github.com/rantav/hector/tree/master/core/src/main/java/me/prettyprint/hector/api
 * @see https://github.com/rantav/hector/wiki/Getting-started-%285-minutes%29
 * 
 * cassandra tutorial by datastax:
 * @see https://github.com/zznate/cassandra-tutorial
 */
public class CassandraDAOImpl extends CassandraBase {

    /**
     * Internal Enum
     */
    public static enum Type {
        STRING,
        LONG,
        BYTE_ARRAY
    };

    // default constructor
    public CassandraDAOImpl() {
        super();
    }

    /**
     * Deletes a key and all of the related columns
     * @param id column key
     * @throws HectorException
     */
    public void deletePerson(String id) throws HectorException {
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
        mutator.delete(id, COLUMNFAMILY_EMP, column, stringSerializer);
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

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUMNFAMILY_EMP, stringSerializer,
                stringSerializer);

        ColumnFamilyUpdater<String, String> updater = template.createUpdater(id);

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
     * Inserts an entire entity to Employee column family
     * @param person person bean
     * @throws HectorException
     */
    public void save(final Person person) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUMNFAMILY_EMP, stringSerializer,
                stringSerializer);

        ColumnFamilyUpdater<String, String> updater = template.createUpdater(person.getId());
        updater.setString("name", person.getName());
        updater.setString("email", person.getEmail());
        updater.setString("login", person.getLogin());
        updater.setString("passwd", person.getPasswd());
        updater.setLong("creation", person.getCreationDate().getTime());

        template.update(updater);
    }

    /**
     * Get a person related with some column key
     * @param id the key
     * @return a filled User bean
     * @throws HectorException
     */
    public Person getPerson(final String id) throws HectorException {

        Person person = null;
        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUMNFAMILY_EMP, stringSerializer,
                stringSerializer);

        ColumnFamilyResult<String, String> res = template.queryColumns(id);

        if (res.hasResults()) {
            person = new Person();
            person.setId(id);
            person.setName(res.getString("name"));
            person.setEmail(res.getString("email"));
            person.setLogin(res.getString("login"));
            person.setPasswd(res.getString("passwd"));
            person.setCreationDate(res.getLong("creation") != null ? new Date(res.getLong("creation")) : null);
        }

        return person;
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

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUMNFAMILY_EMP, stringSerializer,
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
    
    
    public HColumn<String, ByteBuffer> getColumn(final String id, String columnkey) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUMNFAMILY_EMP, stringSerializer,
                stringSerializer);

        HColumn<String, ByteBuffer> column = null;
        
        ColumnFamilyResult<String, String> res = template.queryColumns(id);
        if (res.hasResults()) {
            column = res.getColumn(columnkey);
        }
        
        return column;
    }
}
