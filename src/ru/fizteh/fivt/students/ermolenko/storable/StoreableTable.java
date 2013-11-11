package ru.fizteh.fivt.students.ermolenko.storable;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class StoreableTable implements Table {

    private List<Class<?>> columnOfTypes;
    private StoreableTableProvider tableProvider;
    private Map<String, MyStoreable> dataBase;
    private Map<String, MyStoreable> changesBase;
    private File dataFile;
    private int sizeTable;


    public StoreableTable(File inFile, StoreableTableProvider inTableProvider) throws IOException {

        columnOfTypes = new ArrayList<Class<?>>();
        tableProvider = inTableProvider;
        dataBase = new HashMap<String, MyStoreable>();
        changesBase = new HashMap<String, MyStoreable>();
        dataFile = inFile;
        File signatureFile = new File(inFile, "signature.tsv");
        StoreableUtils.readSignature(signatureFile, columnOfTypes);
    }

    @Override
    public String getName() {

        return dataFile.getName();
    }

    @Override
    public MyStoreable get(String key) {

        if (key == null) {
            throw new IllegalArgumentException("Incorrect key to get.");
        }
        String newKey = key.trim();
        if (newKey.isEmpty()) {
            throw new IllegalArgumentException("Incorrect key to get");
        }
        MyStoreable returnValue;
        if (changesBase.containsKey(newKey)) {
            if (changesBase.get(newKey) == null) {
                returnValue = null;
            } else {
                returnValue = changesBase.get(newKey);
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

    @Override
    public MyStoreable put(String key, Storeable value) throws ColumnFormatException {

        if (key == null || value == null) {
            throw new IllegalArgumentException("Incorrect key or value to put.");
        }

        if ((!changesBase.containsKey(key) && !dataBase.containsKey(key)) ||
                (changesBase.containsKey(key) && changesBase.get(key) == null)) {
            ++sizeTable;
        }
        MyStoreable result = get(key);
        changesBase.put(key, (MyStoreable) value);
        if (value.equals(dataBase.get(key))) {
            changesBase.remove(key);
        }

        return result;
    }

    @Override
    public MyStoreable remove(String key) {

        if (key == null) {
            throw new IllegalArgumentException("Incorrect key to remove.");
        }

        String newKey = key.trim();
        if (newKey.isEmpty()) {
            throw new IllegalArgumentException("Incorrect key to remove");
        }

        if (changesBase.get(newKey) != null || (!changesBase.containsKey(newKey) && dataBase.get(newKey) != null)) {
            --sizeTable;
        }
        MyStoreable result = get(newKey);
        changesBase.put(newKey, null);
        if (dataBase.get(newKey) == null) {
            changesBase.remove(newKey);
        }
        return result;
    }

    @Override
    public int size() {

        return sizeTable;
    }

    @Override
    public int commit() throws IOException {

        int size = changesBase.size();
        try {
            if (size != 0) {
                Set<Map.Entry<String, MyStoreable>> set = changesBase.entrySet();
                for (Map.Entry<String, MyStoreable> pair : set) {
                    pair.getKey();
                    if (pair.getValue() == null) {
                        dataBase.remove(pair.getKey());
                    } else {
                        dataBase.put(pair.getKey(), pair.getValue());
                    }
                }
                StoreableUtils.write(dataFile, this, dataBase, tableProvider);
            }
        } catch (IOException e) {
            System.err.println(e);
        }

        changesBase.clear();
        sizeTable = dataBase.size();
        return size;
    }

    @Override
    public int rollback() {

        int size = changesBase.size();
        changesBase.clear();
        sizeTable = dataBase.size();
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

        return changesBase.size();
    }

    public StoreableTableProvider getTableProvider() {

        return tableProvider;
    }

    public Map<String, MyStoreable> getDataBase() {

        return dataBase;
    }

    public File getDataFile() {

        return dataFile;
    }

    public List<Class<?>> getColumnOfTypes() {

        return columnOfTypes;
    }

    public void changeCurrentTable(List<Class<?>> inColumnOfTypes, StoreableTableProvider inProvider,
                                   Map<String, MyStoreable> inDataBase, File inFile) {

        columnOfTypes = inColumnOfTypes;
        tableProvider = inProvider;
        dataBase = inDataBase;
        dataFile = inFile;
        sizeTable = dataBase.size();
    }
}
