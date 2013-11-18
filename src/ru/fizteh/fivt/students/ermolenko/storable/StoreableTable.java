package ru.fizteh.fivt.students.ermolenko.storable;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StoreableTable implements Table {

    private List<Class<?>> columnOfTypes;
    private StoreableTableProvider tableProvider;
    private HashMap<String, Storeable> dataBase;
    private ThreadLocal<HashMap<String, Storeable>> changesBase;
    private File dataFile;
    private ThreadLocal<Integer> sizeTable;
    protected final Lock tableLock = new ReentrantLock(true);

    public StoreableTable(File inFile, StoreableTableProvider inTableProvider) throws IOException {

        columnOfTypes = new ArrayList<Class<?>>();
        tableProvider = inTableProvider;

        dataBase = new HashMap<String, Storeable>();

        changesBase = new ThreadLocal<HashMap<String, Storeable>>() {
            public HashMap<String, Storeable> initialValue() {
                return new HashMap<String, Storeable>();
            }
        };
        sizeTable = new ThreadLocal<Integer>() {
            public Integer initialValue() {
                return 0;
            }
        };

        dataFile = inFile;
        File signatureFile = new File(inFile, "signature.tsv");
        StoreableUtils.readSignature(signatureFile, columnOfTypes);
    }

    @Override
    public String getName() {

        return dataFile.getName();
    }

    @Override
    public Storeable get(String key) {

        if (key == null) {
            throw new IllegalArgumentException("Incorrect key to get.");
        }
        String newKey = key.trim();
        if (newKey.trim().isEmpty() || key.matches("(.+\\s+.+)+")) {
            throw new IllegalArgumentException("Incorrect key to get");
        }

        Storeable returnValue;
        if (changesBase.get().containsKey(newKey)) {
            if (changesBase.get().get(newKey) == null) {
                returnValue = null;
            } else {
                returnValue = changesBase.get().get(newKey);
            }
        } else {
            if (dataBase.containsKey(newKey)) {
                returnValue = dataBase.get(newKey);
            } else {
                returnValue = null;
            }
        }

        return returnValue;
    }

    public boolean checkExtraColumns(Storeable value) {

        try {
            value.getColumnAt(columnOfTypes.size());
        } catch (IndexOutOfBoundsException e) {
            return true;
        }
        return false;
    }

    @Override
    public Storeable put(String key, Storeable value) throws ColumnFormatException {

        if (key == null || value == null) {
            throw new IllegalArgumentException("Incorrect key or value to put.");
        }

        if (!checkExtraColumns(value)) {
            throw new ColumnFormatException("extra columns found");
        }

        try {
            int size = columnOfTypes.size();
            for (int i = 0; i < size; ++i) {
                if (value.getColumnAt(i) != null && !columnOfTypes.get(i).equals(value.getColumnAt(i).getClass())) {
                    throw new ColumnFormatException("angry storeable");
                }
            }
        } catch (Exception e) {
            throw new ColumnFormatException("less number of columns");
        }

        if ((!changesBase.get().containsKey(key) && !dataBase.containsKey(key)) ||
                (changesBase.get().containsKey(key) && changesBase.get().get(key) == null)) {
            sizeTable.set(sizeTable.get() + 1);
        }
        Storeable result = get(key);
        if (changesBase.get().containsKey(key) && changesBase.get().get(key) == null) {
            changesBase.get().remove(key);
        } else {
            changesBase.get().put(key, value);
        }
        /*
        if (value.equals(dataBase.get(key))) {
            changesBase.get().remove(key);
        }
        */
        return result;
    }

    @Override
    public Storeable remove(String key) {

        if (key == null) {
            throw new IllegalArgumentException("Incorrect key to remove.");
        }

        String newKey = key.trim();
        if (newKey.isEmpty()) {
            throw new IllegalArgumentException("Incorrect key to remove");
        }


        if (changesBase.get().get(newKey) != null || (!changesBase.get().containsKey(newKey) && dataBase.get(newKey) != null)) {
            sizeTable.set(sizeTable.get() - 1);
        }
        Storeable result = get(newKey);
        changesBase.get().put(newKey, null);
        if (dataBase.get(newKey) == null) {
            changesBase.get().remove(newKey);
        }
        return result;
    }

    @Override
    public int size() {

        tableLock.lock();
        try {
            return (sizeTable.get() + dataBase.size());
        } finally {
            tableLock.unlock();
        }
    }

    @Override
    public int commit() throws IOException {

        tableLock.lock();
        try {
            int size = changesBase.get().size();
            try {
                if (size != 0) {
                    Set<Map.Entry<String, Storeable>> set = changesBase.get().entrySet();
                    for (Map.Entry<String, Storeable> pair : set) {
                        pair.getKey();
                        if (pair.getValue() == null) {
                            dataBase.remove(pair.getKey());
                        } else {
                            if (dataBase.get(pair.getKey()) != null) {
                                if (!dataBase.get(pair.getKey()).equals(pair.getValue())) {
                                    dataBase.put(pair.getKey(), pair.getValue());
                                } else {
                                    --size;
                                }
                            } else {
                                dataBase.put(pair.getKey(), pair.getValue());
                            }
                        }
                    }
                    StoreableUtils.write(dataFile, this, dataBase, tableProvider);
                }
            } catch (IOException e) {
                System.err.println(e);
            }
            changesBase.get().clear();
            sizeTable.set(0);
            //sizeTable.set(dataBase.size());
            return size;
        } finally {
            tableLock.unlock();
        }
    }

    @Override
    public int rollback() {

        int size = changesBase.get().size();
        Set<Map.Entry<String, Storeable>> set = changesBase.get().entrySet();
        for (Map.Entry<String, Storeable> pair : set) {
            if (dataBase.containsKey(pair.getKey())) {
                if (dataBase.get(pair.getKey()) == pair.getValue()) {
                    --size;
                }
            }
        }
        changesBase.get().clear();
        sizeTable.set(dataBase.size());
        return size;
    }

    @Override
    public int getColumnsCount() {

        return columnOfTypes.size();
    }

    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {

        return columnOfTypes.get(columnIndex);
    }

    public int getChangesBaseSize() {

        return changesBase.get().size();
    }

    public StoreableTableProvider getTableProvider() {

        return tableProvider;
    }

    public HashMap<String, Storeable> getDataBase() {

        return dataBase;
    }

    public File getDataFile() {

        return dataFile;
    }

    public List<Class<?>> getColumnOfTypes() {

        return columnOfTypes;
    }

    public void changeCurrentTable(List<Class<?>> inColumnOfTypes, StoreableTableProvider inProvider,
                                   HashMap<String, Storeable> inDataBase, File inFile) {

        columnOfTypes = inColumnOfTypes;
        tableProvider = inProvider;
        dataBase = inDataBase;
        dataFile = inFile;
        sizeTable.set(0);
    }
}
