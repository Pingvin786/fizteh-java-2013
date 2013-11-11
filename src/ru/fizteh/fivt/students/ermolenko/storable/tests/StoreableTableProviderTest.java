package ru.fizteh.fivt.students.ermolenko.storable.tests;

import org.junit.*;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.students.ermolenko.multifilehashmap.MultiFileHashMapUtils;
import ru.fizteh.fivt.students.ermolenko.storable.MyStoreable;
import ru.fizteh.fivt.students.ermolenko.storable.StoreableTable;
import ru.fizteh.fivt.students.ermolenko.storable.StoreableTableProvider;
import ru.fizteh.fivt.students.ermolenko.storable.StoreableTableProviderFactory;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: evgenij
 * Date: 10.11.13
 * Time: 0:55
 * To change this template use File | Settings | File Templates.
 */
public class StoreableTableProviderTest {

    private static StoreableTableProvider tableProvider;
    private static List<Class<?>> goodList = new ArrayList<Class<?>>();
    private static List<Class<?>> badList = new ArrayList<Class<?>>();
    private static List<Class<?>> emptyList = new ArrayList<Class<?>>();
    private static StoreableTable table;
    private static String testString;

    @BeforeClass
    public static void setUpClass() throws Exception {

        StoreableTableProviderFactory tableProviderFactory = new StoreableTableProviderFactory();
        tableProvider = tableProviderFactory.create("javatest");
        goodList.add(Integer.class);
        goodList.add(Byte.class);
        goodList.add(Long.class);
        goodList.add(Float.class);
        goodList.add(Double.class);
        goodList.add(String.class);
        goodList.add(Boolean.class);
        testString = "<row><col>5</col><col>0</col><col>65777</col><col>" +
                "5.5</col><col>767.576</col><col>frgedr</col><col>true</col></row>";
        badList.add(null);
        badList.add(IllegalArgumentException.class);
    }

    @Before
    public void setUp() throws Exception {

        table = tableProvider.createTable("table", goodList);
    }

    @After
    public void tearDown() throws Exception {

        tableProvider.removeTable("table");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {

        File file = new File("javatest");
        if (file.exists()) {
            MultiFileHashMapUtils.deleteDirectory(file);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableNullName() throws Exception {

        tableProvider.createTable(null, goodList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableEmptyName() throws Exception {

        tableProvider.createTable("", goodList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableIncorrectName() throws Exception {

        tableProvider.createTable("@#$@%", goodList);
    }

    @Test
    public void testCreateTableExisted() throws Exception {

        tableProvider.createTable("testCreateTableExisted", goodList);
        Assert.assertNull(tableProvider.createTable("testCreateTableExisted", goodList));
        tableProvider.removeTable("testCreateTableExisted");
    }

    @Test
    public void testCreateTableNotExisted() throws Exception {

        Assert.assertNotNull(tableProvider.createTable("testCreateTableNotExisted", goodList));
        tableProvider.removeTable("testCreateTableNotExisted");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableNullTypeList() throws Exception {

        tableProvider.createTable("testCreateTableNullTypeList", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableEmptyTypeList() throws Exception {

        tableProvider.createTable("testCreateTableEmptyTypeList", emptyList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableIncorrectTypeList() throws Exception {

        tableProvider.createTable("testCreateTableIncorrectTypeList", badList);
    }

    @Test(expected = ParseException.class)
    public void testDeserializeWrongFormat() throws Exception {

        tableProvider.deserialize(table, "tfoxhncvgixf");
    }

    @Test
    public void testDeserializeRightFormat() throws Exception {

        Assert.assertNotNull(tableProvider.deserialize(table, testString));
    }

    @Test
    public void testSerialize() throws Exception {

        MyStoreable storeable = tableProvider.deserialize(table, testString);
        Assert.assertEquals(testString, tableProvider.serialize(table, storeable));
    }

    @Test
    public void testCreateForTable() throws Exception {

        Assert.assertNotNull(tableProvider.createFor(table));
    }

    @Test
    public void testCreateForTableAndGoodValues() throws Exception {

        List<Object> values = new ArrayList<Object>();
        values.add(5);
        values.add(Byte.valueOf((byte) 0));
        values.add(Long.valueOf(47456));
        values.add(Float.valueOf((float) 5.5));
        values.add(5.5);
        values.add("jhfkflg");
        values.add(true);
        Assert.assertNotNull(tableProvider.createFor(table, values));
    }

    @Test(expected = ColumnFormatException.class)
    public void testCreateForTableAndBadValuesColumnFormat() throws Exception {

        List<Object> values = new ArrayList<Object>();
        values.add(5);
        values.add(0);
        values.add("fkjbdflg");
        values.add(5.5);
        values.add(5.5);
        values.add("jhfkflg");
        values.add(true);
        tableProvider.createFor(table, values);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testCreateForTableAndBadValuesOutOfBounds() throws Exception {

        List<Object> values = new ArrayList<Object>();
        values.add(5);
        values.add(0);
        values.add(47456);
        values.add(5.5);
        values.add(3758.9875);
        values.add("jhfkflg");
        values.add(true);
        values.add("blabla");
        tableProvider.createFor(table, values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableNull() throws Exception {

        tableProvider.getTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableEmpty() throws Exception {

        tableProvider.getTable("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableIncorrect() throws Exception {

        tableProvider.getTable("@#$@%");
    }

    @Test
    public void testGetTableExisted() throws Exception {

        StoreableTable table1 = tableProvider.createTable("testGetTableExisted", goodList);
        Assert.assertEquals(table1, tableProvider.getTable("testGetTableExisted"));
    }

    @Test
    public void testGetTableNotExisted() throws Exception {

        Assert.assertNull(tableProvider.getTable("testGetTableNotExisted"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveTableNull() throws Exception {

        tableProvider.removeTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveTableEmpty() throws Exception {

        tableProvider.removeTable("");
    }

    @Test(expected = IllegalStateException.class)
    public void testRemoveTableNotExisted() throws Exception {

        tableProvider.removeTable("testRemoveTableNotExisted");
    }

    @Test
    public void testRemoveTableExisted() throws Exception {

        StoreableTable table1 = tableProvider.createTable("testRemoveTableExisted", goodList);
        tableProvider.removeTable("testRemoveTableExisted");
        Assert.assertNull(tableProvider.getTable("testRemoveTableExisted"));
    }
}
