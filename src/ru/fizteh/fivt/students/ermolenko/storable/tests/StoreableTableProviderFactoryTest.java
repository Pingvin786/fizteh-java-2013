package ru.fizteh.fivt.students.ermolenko.storable.tests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import ru.fizteh.fivt.students.ermolenko.multifilehashmap.MultiFileHashMapUtils;
import ru.fizteh.fivt.students.ermolenko.storable.StoreableTableProviderFactory;

import java.io.File;

public class StoreableTableProviderFactoryTest {

    private StoreableTableProviderFactory tableProviderFactory = new StoreableTableProviderFactory();

    @After
    public void tearDown() throws Exception {

        File file = new File("javatest");
        if (file.exists()) {
            MultiFileHashMapUtils.deleteDirectory(file);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateNull() throws Exception {

        tableProviderFactory.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateEmpty() throws Exception {

        tableProviderFactory.create("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateNl() throws Exception {

        tableProviderFactory.create("    ");
    }

    @Test
    public void testCreateNotExisted() throws Exception {

        Assert.assertNotNull(tableProviderFactory.create("javatest"));
    }
}
