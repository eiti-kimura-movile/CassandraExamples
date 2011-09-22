package com.movile.tests;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Assert;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;

import org.apache.log4j.xml.DOMConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.movile.bean.Person;
import com.movile.cassandra.CassandraDAOImpl;
import com.movile.utils.AppProperties;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 */
public class CassadraTest {

    private static CassandraDAOImpl manager;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // initializing resouces, logs property files and etc...
        DOMConfigurator.configure("conf/log/log4j.xml");
        AppProperties.getDefaultInstance().loadProperties("conf/const.properties");

        manager = new CassandraDAOImpl();
        setupDataSet();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {

        clearDataSet();

        // finishes resource
        manager.shutdown();
    }

    /**
     * Basic test case to check if some the object was updated
     */
    @Test
    public void updateObject() {
        Person jared = manager.getPerson("jared86");
        String email = "newmail@mail.com";

        // set a new e-mail
        jared.setEmail("newmail@mail.com");
        manager.save(jared);

        Assert.assertEquals(email, manager.getPerson("jared86").getEmail());
    }

    /**
     * Basic test case to check if some the object was updated
     */
    @Test
    public void updateObjectV2() {
        Person jared = manager.getPerson("jared86");
        String email = "newmail@mail.com";

        // set a new e-mail
        jared.setEmail("newmail@mail.com");
        manager.saveV2(jared);

        Assert.assertEquals(email, manager.getPerson("jared86").getEmail());
    }

    /**
     * Basic test case to check if some the column was updated
     */
    @Test
    public void updateColumn() {
        HColumn<String, ByteBuffer> column = manager.getColumn("cloe79", "name");
        Assert.assertNotNull(column);

        // check the value
        Assert.assertEquals("Cloe Anderson", StringSerializer.get().fromByteBuffer(column.getValue()));

        manager.update("cloe79", "name", "Cloe Matthews", CassandraDAOImpl.Type.STRING);

        // get the column value again
        column = manager.getColumn("cloe79", "name");
        Assert.assertNotNull(column);

        // compare with the new value
        Assert.assertEquals("Cloe Matthews", StringSerializer.get().fromByteBuffer(column.getValue()));
    }

    /**
     * Basic test case to check if some the column was updated
     */
    @Test
    public void updateColumnV2() {
        HColumn<String, ByteBuffer> column = manager.getColumn("cloe79", "name");
        Assert.assertNotNull(column);

        // check the value
        Assert.assertEquals("Cloe Anderson", StringSerializer.get().fromByteBuffer(column.getValue()));

        manager.updateColumn("cloe79", "name", "Cloe Matthews", CassandraDAOImpl.Type.STRING);

        // get the column value again
        column = manager.getColumn("cloe79", "name");
        Assert.assertNotNull(column);

        // compare with the new value
        Assert.assertEquals("Cloe Matthews", StringSerializer.get().fromByteBuffer(column.getValue()));
    }

    /**
     * Check if the timestamp changes for the other columns when some of them was updated
     */
    @Test
    public void updateColumnCheckTimestamp() {

        String key = "suzy84";

        HColumn<String, ByteBuffer> column = manager.getColumn(key, "name");
        long nameTs = column.getClock();

        column = manager.getColumn(key, "email");
        long emailTs = column.getClock();

        column = manager.getColumn(key, "login");
        long loginTs = column.getClock();

        column = manager.getColumn(key, "passwd");
        long passTs = column.getClock();

        column = manager.getColumn(key, "creation");
        long creationTs = column.getClock();

        manager.update(key, "name", "Syze the Doll. Yes, I'm a toy", CassandraDAOImpl.Type.STRING);
        manager.update(key, "email", "new.mail@mail.com", CassandraDAOImpl.Type.STRING);

        // updated columns the timestamp was changed
        column = manager.getColumn(key, "name");
        Assert.assertNotSame(nameTs, column.getClock());

        column = manager.getColumn(key, "email");
        Assert.assertNotSame(emailTs, column.getClock());

        // untouched columns
        column = manager.getColumn(key, "login");
        Assert.assertEquals(loginTs, column.getClock());

        column = manager.getColumn(key, "passwd");
        Assert.assertEquals(passTs, column.getClock());

        column = manager.getColumn(key, "creation");
        Assert.assertEquals(creationTs, column.getClock());
    }

    /**
     * Check if the timestamp changes for the other columns when some of them was updated
     */
    @Test
    public void updateColumnCheckTimestampV2() {

        String key = "suzy84";

        HColumn<String, ByteBuffer> column = manager.getColumn(key, "name");
        long nameTs = column.getClock();

        column = manager.getColumn(key, "email");
        long emailTs = column.getClock();

        column = manager.getColumn(key, "login");
        long loginTs = column.getClock();

        column = manager.getColumn(key, "passwd");
        long passTs = column.getClock();

        column = manager.getColumn(key, "creation");
        long creationTs = column.getClock();

        Assert.assertNotSame(nameTs, manager.updateColumn(key, "name", "Syze the Doll. Yes, I'm a toy", CassandraDAOImpl.Type.STRING));
        Assert.assertNotSame(emailTs, manager.updateColumn(key, "email", "new.mail@mail.com", CassandraDAOImpl.Type.STRING));

        // untouched columns
        column = manager.getColumn(key, "login");
        Assert.assertEquals(loginTs, column.getClock());

        column = manager.getColumn(key, "passwd");
        Assert.assertEquals(passTs, column.getClock());

        column = manager.getColumn(key, "creation");
        Assert.assertEquals(creationTs, column.getClock());
    }

    @Test
    public void concurrencyAndConsistency() throws InterruptedException {

        final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

        HColumn<String, ByteBuffer> column = manager.getColumn("ekm82", "passwd");
        long passTs = column.getClock();

        ExecutorService executor = Executors.newFixedThreadPool(30);
        System.out.println(manager.getPerson("ekm82"));
        
        for (int i = 0; i < 10; i++) {
            
            // create threads
            Runnable task1 = new Runnable() {
                @Override
                public void run() {
                    System.out.println("email - Runnning: " + sdf.format(new Date()));
                    manager.update("ekm82", "email", "my.changed.email.lets.see@mail.com", CassandraDAOImpl.Type.STRING);
                }
            };

            Runnable task2 = new Runnable() {
                @Override
                public void run() {
                    System.out.println("name - Runnning: " + sdf.format(new Date()));
                    manager.update("ekm82", "name", "Joao Paulo Eiti Kimura", CassandraDAOImpl.Type.STRING);
                }
            };
            
            Runnable task3 = new Runnable() {
                @Override
                public void run() {
                    System.out.println("creationDate - Runnning: " + sdf.format(new Date()));
                    manager.update("ekm82", "creation", new Date().getTime(), CassandraDAOImpl.Type.LONG);
                }
            };
            
            // submit tasks to pool
            executor.submit(task1);
            executor.submit(task2);
            executor.submit(task3);
        }

        // start to execute the threads
        executor.shutdown();
        
        while(!executor.isShutdown()) {
            ; //wait threads finish the job
        }
        
        Thread.sleep(5000); //waits for 5 seconds
        
        // get record after updates
        Person person = manager.getPerson("ekm82");
        System.out.println(person);
        
        Assert.assertEquals("Joao Paulo Eiti Kimura", person.getName());
        Assert.assertEquals("my.changed.email.lets.see@mail.com", person.getEmail());

        // check if the timestamp of other column was untouched
        column = manager.getColumn("ekm82", "passwd");
        Assert.assertEquals(passTs, column.getClock());
    }

    /**
     * creates a initial dataset to test
     */
    private static void setupDataSet() {
        manager.save(new Person("jared86", "Jared Polin AKA the FRO", "jared", "552fro", "jared@mail.com"));
        manager.save(new Person("cloe79", "Cloe Anderson", "cloe", "clo24132154312", "cloe@mail.com"));
        manager.save(new Person("suzy84", "Suzy the Doll", "suzy", "wowsu332", "suzy@mail.com"));
        manager.save(new Person("ekm82", "Eiti Kimura", "boom", "mypassword", "eiti@mail.com"));
    }

    /**
     * removes data from cassandra
     */
    private static void clearDataSet() {
        manager.deletePerson("ekm82");
        manager.deletePerson("jared86");
        manager.deletePerson("cloe79");
        manager.deletePerson("suzy84");
    }
}
