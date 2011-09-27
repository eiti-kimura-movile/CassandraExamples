package com.movile.cassandra;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import com.movile.bean.Person;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 * DAO class to handle employee entities
 */
public class EmployeeDAOImpl extends CassandraBase {

    private static final String COLUNM_FAMILY_EMP = "Employees";

    public EmployeeDAOImpl() {

    }

    /**
     * Inserts an entire entity to Employee column family
     * @param person person bean
     * @throws HectorException
     */
    public void save(final Person person) throws HectorException {

        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUNM_FAMILY_EMP, stringSerializer,
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
     * Inserts an entire entity to Employee column family
     * @param person person bean
     * @throws HectorException
     */
    public void saveV2(final Person person) throws HectorException {

        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

        HColumn<String, String> colName = HFactory.createStringColumn("name", person.getName());
        HColumn<String, String> colEmail = HFactory.createStringColumn("email", person.getEmail());
        HColumn<String, String> colLogin = HFactory.createStringColumn("login", person.getLogin());
        HColumn<String, String> colPasswd = HFactory.createStringColumn("passwd", person.getPasswd());
        HColumn<String, Long> colCreation = HFactory.createColumn("creation", person.getCreationDate().getTime(), stringSerializer, longSerializer);

        String key = person.getId();
        mutator.addInsertion(key, COLUNM_FAMILY_EMP, colName);
        mutator.addInsertion(key, COLUNM_FAMILY_EMP, colEmail);
        mutator.addInsertion(key, COLUNM_FAMILY_EMP, colLogin);
        mutator.addInsertion(key, COLUNM_FAMILY_EMP, colPasswd);
        mutator.addInsertion(key, COLUNM_FAMILY_EMP, colCreation);

        mutator.execute();
    }

    /**
     * Get a person related with some column key
     * @param id the key
     * @return a filled User bean
     * @throws HectorException
     */
    public Person getPerson(final String id) throws HectorException {

        Person person = null;
        ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(keyspace, COLUNM_FAMILY_EMP, stringSerializer,
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
     * Get all of records, similar to selet * from Employees (limit 100 rows)
     * @return a list if employees
     */
    public List<Person> getAll() {
        List<Person> employees = new ArrayList<Person>();

        // creates a range query
        RangeSlicesQuery<String, String, Object> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer,
            ObjectSerializer.get());

        // set the query parameters
        rangeSlicesQuery.setColumnFamily(COLUNM_FAMILY_EMP);

        // the start key
        rangeSlicesQuery.setKeys("", ""); // must set the next key for a large database
        rangeSlicesQuery.setRange("", "", false, 150);
        rangeSlicesQuery.setRowCount(100); // limit of rows returned

        // executes the query and get rowns
        QueryResult<OrderedRows<String, String, Object>> result = rangeSlicesQuery.execute();

        OrderedRows<String, String, Object> orderedRows = result.get();

        if (result != null) {
            for (Row<String, String, Object> row : orderedRows.getList()) {
                // get the subscription column to acquire the object bytes
                ColumnSlice<String, Object> columnSlice = row.getColumnSlice();
                Person person = new Person();
                person.setId(row.getKey());
                
                // get column information and set the object
                HColumn<String, Object> column = columnSlice.getColumnByName("name");
                person.setName(stringSerializer.fromByteBuffer(column.getValueBytes()));
                
                column = columnSlice.getColumnByName("email");
                person.setEmail(stringSerializer.fromByteBuffer(column.getValueBytes()));
                
                column = columnSlice.getColumnByName("login");
                person.setLogin(stringSerializer.fromByteBuffer(column.getValueBytes()));
                
                column = columnSlice.getColumnByName("passwd");
                person.setPasswd(stringSerializer.fromByteBuffer(column.getValueBytes()));
                
                column = columnSlice.getColumnByName("creation");
                person.setCreationDate(new Date(longSerializer.fromByteBuffer(column.getValueBytes())));

                employees.add(person);
            }
        }
        return employees;
    }

}
