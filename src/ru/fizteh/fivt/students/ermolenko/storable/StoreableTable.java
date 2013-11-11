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
    private Map<String, Storeable> dataBase;
    private Map<String, Storeable> changesBase;
    private File dataFile;
    private int sizeTable;


    public StoreableTable(File inFile, StoreableTableProvider inTableProvider) throws IOException {

        columnOfTypes = new ArrayList<Class<?>>();
        tableProvider = inTableProvider;
        dataBase = new HashMap<String, Storeable>();
        changesBase = new HashMap<String, Storeable>();
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
            for (int i = 0; i < columnOfTypes.size(); ++i) {
                value.getColumnAt(i);
            }
        } catch (Exception e) {
            throw new ColumnFormatException("less number of columns");
        }

        if ((!changesBase.containsKey(key) && !dataBase.containsKey(key)) ||
                (changesBase.containsKey(key) && changesBase.get(key) == null)) {
            ++sizeTable;
        }
        Storeable result = get(key);
        changesBase.put(key, value);
        if (value.equals(dataBase.get(key))) {
            changesBase.remove(key);
        }

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


        if (changesBase.get(newKey) != null || (!changesBase.containsKey(newKey) && dataBase.get(newKey) != null)) {
            --sizeTable;
        }
        Storeable result = get(newKey);
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
                Set<Map.Entry<String, Storeable>> set = changesBase.entrySet();
                for (Map.Entry<String, Storeable> pair : set) {
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

    public Map<String, Storeable> getDataBase() {

        return dataBase;
    }

    public File getDataFile() {

        return dataFile;
    }

    public List<Class<?>> getColumnOfTypes() {

        return columnOfTypes;
    }

    public void changeCurrentTable(List<Class<?>> inColumnOfTypes, StoreableTableProvider inProvider,
                                   Map<String, Storeable> inDataBase, File inFile) {

        columnOfTypes = inColumnOfTypes;
        tableProvider = inProvider;
        dataBase = inDataBase;
        dataFile = inFile;
        sizeTable = dataBase.size();
    }
}
