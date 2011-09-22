package com.movile.bin;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.xml.DOMConfigurator;

import com.movile.bean.Person;
import com.movile.cassandra.CassandraDAOImpl;
import com.movile.utils.AppProperties;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 */
public final class Main {
    
    private Main() {
        
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        // initializing resouces, logs property files and etc...
        DOMConfigurator.configure("conf/log/log4j.xml");
        AppProperties.getDefaultInstance().loadProperties("conf/const.properties");

        CassandraDAOImpl manager = new CassandraDAOImpl();

        // inserting data to cluster column family
        System.out.println("Inserting data...");
        Person person = new Person("ekm82", "Eiti Kimura", "boom", "mypassword", "eiti@mail.com");
        manager.save(person);
        System.out.println(person);

        // the data struture inside cassandra will be:
        // [default@Company] get Employees['ekm82'];
        // => (column=6372656174696f6e, value=000001328c7020e5, timestamp=1316616159352000)
        // => (column=656d61696c, value=656974692e6b696d757261406d6f76696c652e636f6d, timestamp=1316616158689000)
        // => (column=6c6f67696e, value=656974696b696d757261, timestamp=1316616158878000)
        // => (column=6e616d65, value=45697469204b696d757261, timestamp=1316616158490000)
        // => (column=706173737764, value=6d7970617373776f7264, timestamp=1316616159053000)

        // retrieving data from comlumn family
        System.out.println("Reading data");
        Person personRetrieved = new Person();
        personRetrieved = manager.getPerson("ekm82");
        System.out.println(personRetrieved);

        // finish the resources
        manager.shutdown();
    }

}
