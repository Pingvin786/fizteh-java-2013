package ru.fizteh.fivt.students.ermolenko.filemap;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        String currentProperty = System.getProperty("fizteh.db.dir");
        File base = new File(currentProperty);

        try {
            base = base.getCanonicalFile().toPath().resolve("db.dat").toFile();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        FileMap filemap = new FileMap(base);

        try {
            if (args.length > 0) {
                filemap.batchState(args);
            } else {
                filemap.interactiveState();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        System.exit(0);
    }
}