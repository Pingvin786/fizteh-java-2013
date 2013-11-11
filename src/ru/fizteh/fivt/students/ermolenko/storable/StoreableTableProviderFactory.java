package ru.fizteh.fivt.students.ermolenko.storable;

import ru.fizteh.fivt.storage.structured.TableProviderFactory;

import java.io.File;
import java.io.IOException;

public class StoreableTableProviderFactory implements TableProviderFactory {

    @Override
    public StoreableTableProvider create(String path) throws IOException {

        if (path == null) {
            throw new IllegalArgumentException("value of path is null");
        }

        if (path.trim().isEmpty()) {
            throw new IOException("directory is empty");
        }

        File file = new File(path);
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("should be directory");
        }

        return new StoreableTableProvider(new File(path));
    }
}
