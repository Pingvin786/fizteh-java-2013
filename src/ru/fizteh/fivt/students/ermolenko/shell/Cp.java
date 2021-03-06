package ru.fizteh.fivt.students.ermolenko.shell;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Cp implements Command<ShellState> {

    public String getName() {

        return "cp";
    }

    private void copy(Path source, Path target) throws IOException {

        target = target.resolve(source.getFileName());
        if (source.toFile().isFile()) {
            Files.copy(source, target);
        } else {
            File[] masOfSource = source.toFile().listFiles();
            target.toFile().mkdir();
            for (File sourceEntry : masOfSource != null ? masOfSource : new File[0]) {
                copy(sourceEntry.toPath(), target);
            }
        }
    }

    public void executeCmd(ShellState inState, String[] args) throws IOException {

        if (2 == args.length) {
            Path source = inState.getPath().resolve(args[0]).normalize();
            Path target = inState.getPath().resolve(args[1]).normalize();
            if (source.toFile().isFile() && target.toFile().isFile()) {
                throw new IOException("not allowed to copy file to file");
            }
            if (source.equals(target)) {
                throw new IOException("not allowed to copy file on itself");
            }
            if (!source.toFile().exists()) {
                throw new IOException("source file doesn't exist");
            }
            if (source.toFile().isDirectory() && target.toFile().isFile()) {
                throw new IOException("not allowed to copy directory in file");
            }
            if (source.toFile().isDirectory() && !target.toFile().exists()) {
                throw new IOException("target directory doesn't exist");
            }
            if (source.toFile().isDirectory() && target.toFile().isDirectory() && target.startsWith(source)) {
                throw new IOException("not allowed to copy parent directory in kid's directory");
            }

            if (source.toFile().isFile() && !target.toFile().exists()) {
                Files.copy(source, target);
            } else if (source.toFile().isFile() && target.toFile().isDirectory()) {
                Files.copy(source, target.resolve(source.getFileName()));
            } else if (source.toFile().isDirectory() && target.toFile().isDirectory()) {
                File[] masOfSource = source.toFile().listFiles();
                target.toFile().mkdir();
                if (masOfSource != null) {
                    for (File sourceEntry : masOfSource) {
                        copy(sourceEntry.toPath(), target);
                    }
                }
            }
        } else {
            throw new IOException("not allowed number of arguments");
        }
    }
}
