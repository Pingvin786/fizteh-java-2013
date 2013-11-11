package ru.fizteh.fivt.students.ermolenko.storable;

import ru.fizteh.fivt.students.ermolenko.shell.Command;

import java.io.File;
import java.io.IOException;

public class CmdDrop implements Command<StoreableState> {

    @Override
    public String getName() {

        return "drop";
    }

    @Override
    public void executeCmd(StoreableState inState, String[] args) throws IOException {

        if (inState.getTable(args[0]) == null) {
            throw new IllegalStateException(args[0] + " not exists");
        } else {
            inState.deleteTable(args[0]);
            File db = inState.getTable(args[0]).getDataFile().getCanonicalFile();
            if (!db.exists()) {
                System.out.println(args[0] + " not exists");
            } else {
                if (db.listFiles() != null) {
                    for (File dbDir : db.listFiles()) {
                        if (dbDir.listFiles() != null) {
                            for (File entry : dbDir.listFiles()) {
                                entry.delete();
                            }
                        }
                        dbDir.delete();
                    }
                }
                System.out.println("dropped");
            }
        }
    }
}
