
package com.movile.cassandra;

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
    
}
