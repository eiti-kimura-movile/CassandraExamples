package com.movile.tests;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
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
import com.movile.cassandra.EmployeeDAOImpl;
import com.movile.utils.AppProperties;

/**
 * @author J.P. Eiti Kimura (eiti.kimura@movile.com)
 */
public class CassadraTest {

    private static CassandraDAOImpl manager;
    private static EmployeeDAOImpl empDAO;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // initializing resouces, logs property files and etc...
        DOMConfigurator.configure("conf/log/log4j.xml");
        AppProperties.getDefaultInstance().loadProperties("conf/const.properties");

        manager = new CassandraDAOImpl("Employees");
        empDAO = new EmployeeDAOImpl();
        
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
        Person jared = empDAO.getPerson("jared86");
        String email = "newmail@mail.com";

        // set a new e-mail
        jared.setEmail("newmail@mail.com");
        empDAO.save(jared);

        Assert.assertEquals(email, empDAO.getPerson("jared86").getEmail());
    }

    /**
     * Basic test case to check if some the object was updated
     */
    @Test
    public void updateObjectV2() {
        Person jared = empDAO.getPerson("jared86");
        String email = "newmail@mail.com";

        // set a new e-mail
        jared.setEmail("newmail@mail.com");
        empDAO.saveV2(jared);

        Assert.assertEquals(email, empDAO.getPerson("jared86").getEmail());
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
        
        // back original value
        manager.update("cloe79", "name", "Cloe Anderson", CassandraDAOImpl.Type.STRING);
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

    
    /**
     * Checking consistency, the same row being updated by a bunch of parallel threads
     * @throws InterruptedException
     */
    @Test
    public void concurrencyAndConsistency() throws InterruptedException {

        final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        final String key = "joe92";
        
        // get timestamp for a column that will not be updated
        HColumn<String, ByteBuffer> column = manager.getColumn(key, "passwd");
        long passTs = column.getClock();

        // creates a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(30);
        System.out.println(empDAO.getPerson(key));
        
        for (int i = 0; i < 10; i++) {
            
            // create threads
            Runnable task1 = new Runnable() {
                @Override
                public void run() {
                    System.out.println("email - Runnning: " + sdf.format(new Date()));
                    manager.update(key, "email", "my.changed.email.lets.see@mail.com", CassandraDAOImpl.Type.STRING);
                }
            };

            Runnable task2 = new Runnable() {
                @Override
                public void run() {
                    System.out.println("name - Runnning: " + sdf.format(new Date()));
                    manager.update(key, "name", "Joe Robhert (Changed Name)", CassandraDAOImpl.Type.STRING);
                }
            };
            
            Runnable task3 = new Runnable() {
                @Override
                public void run() {
                    System.out.println("creationDate - Runnning: " + sdf.format(new Date()));
                    manager.update(key, "creation", new Date().getTime(), CassandraDAOImpl.Type.LONG);
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
        
        Thread.sleep(1000); //waits for 1 second
        
        // get record after updates
        Person person = empDAO.getPerson(key);
        System.out.println(person);
        
        Assert.assertEquals("Joe Robhert (Changed Name)", person.getName());
        Assert.assertEquals("my.changed.email.lets.see@mail.com", person.getEmail());

        // check if the timestamp of other column was untouched
        column = manager.getColumn(key, "passwd");
        Assert.assertEquals(passTs, column.getClock());
    }
  
    
    /**
     * Test iteration over all of some column family internal columns, without exactly known its names (keys)
     * @throws InterruptedException
     */
    @Test 
    public void simulateMessageBoard() throws InterruptedException {

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        CassandraDAOImpl managerMessageBoard = new CassandraDAOImpl("MessageBoard");
        
        // clear the today's message board
        managerMessageBoard.delete("Today");
        
        // insert some data
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime()), "ekm82: Hello Everyone, today is a sunny day, nice job for everyone", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 50000), "cloe79: How are you guys?", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 10000), "suzy84: I wish a great day for all of you!", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 150000), "jared86: What's up doc?", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 4000), "jared86: Where are Juliet?", CassandraDAOImpl.Type.STRING);

        Map<String, String> posts = managerMessageBoard.getColumns("Today");

        //wait a little
        Thread.sleep(3000);
        
        //simulate more chat conversation
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime()), "suzy84: Great message board I liked it", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() - 50000), "unknown: what is happening here?", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 10000), "ekm82: Do you think that these messages are being stored?", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 50000), "cloe79: Today is friday!", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 150000), "jared86: Let's practice a little!", CassandraDAOImpl.Type.STRING);
        managerMessageBoard.update("Today", String.valueOf(new Date().getTime() + 900000), "joe92: Let's go to the weekend!", CassandraDAOImpl.Type.STRING);

        // read all of the columns of key: Today
        posts = managerMessageBoard.getColumns("Today");
        
        for (Entry<String,String> entry : posts.entrySet()) {
            String message = sdf.format(new Date(Long.parseLong(entry.getKey())));
            message += " - " + entry.getValue();
            System.out.println(message);
        }
        
        // check the message board size
        Assert.assertEquals(11,posts.entrySet().size());
    }
    
    @Test
    public void removeColumn() {
        
        Map<String, String> mapState1 = manager.getColumns("joe92");
        
        // check the column existence
        Assert.assertNotNull(manager.getColumn("joe92", "name"));
        
        // remove column
        manager.deleteColumn("joe92", "name");
        
        Map<String, String> mapState2 = manager.getColumns("joe92");
        
        // check if the number of columns were decreased
        Assert.assertTrue(mapState1.size() > mapState2.size());
        
        // check if column goes away
        Assert.assertNull(manager.getColumn("joe92", "name"));
    }
    
    /**
     * creates a initial dataset to test
     */
    private static void setupDataSet() {
        empDAO.save(new Person("jared86", "Jared Polin AKA the FRO", "jared", "552fro", "jared@mail.com"));
        empDAO.save(new Person("cloe79", "Cloe Anderson", "cloe", "clo24132154312", "cloe@mail.com"));
        empDAO.save(new Person("suzy84", "Suzy the Doll", "suzy", "wowsu332", "suzy@mail.com"));
        empDAO.save(new Person("ekm82", "Eiti Kimura", "boom", "mypassword", "eiti@mail.com"));
        empDAO.save(new Person("joe92", "Joe Robertson", "joe", "oed@43##", "joe@mail.com"));
    }

    /**
     * removes data from cassandra
     */
    private static void clearDataSet() {
        manager.delete("ekm82");
        manager.delete("jared86");
        manager.delete("cloe79");
        manager.delete("suzy84");
        manager.delete("joe92");
    }
}
