package ru.fizteh.fivt.students.ermolenko.storable;

import ru.fizteh.fivt.storage.structured.Storeable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StoreableState {

    private StoreableTableProvider provider;
    private StoreableTable currentTable;

    public StoreableState(File inFile) throws IOException {

        currentTable = null;
        StoreableTableProviderFactory factory = new StoreableTableProviderFactory();
        provider = factory.create(inFile.getPath());
    }

    public StoreableTable getTable(String name) {

        return provider.getTable(name);
    }

    public StoreableTable getCurrentTable() {

        return currentTable;
    }

    public int getChangesBaseSize() {

        return currentTable.getChangesBaseSize();
    }

    public StoreableTableProvider getProvider() {

        return provider;
    }

    public void setCurrentTable(String name, List<Class<?>> columnOfTypes, StoreableTableProvider provider,
                                Map<String, Storeable> dataBase, File file) {

        currentTable = provider.getTable(name);
        currentTable.changeCurrentTable(columnOfTypes, provider, dataBase, file);

    }

    public StoreableTable createTable(String name, List<Class<?>> inColumnTypes) throws IOException {

        return provider.createTable(name, inColumnTypes);
    }

    public void deleteTable(String name) throws IOException {

        provider.removeTable(name);
        //MultiFileHashMapUtils.deleteDirectory(provider.getTable(name).getDataFile());
        currentTable = null;
    }

    public Storeable putToCurrentTable(String key, Storeable value) {

        return currentTable.put(key, value);
    }

    public Storeable getFromCurrentTable(String key) {

        return currentTable.get(key);
    }

    public Storeable removeFromCurrentTable(String key) {

        return currentTable.remove(key);
    }
}
