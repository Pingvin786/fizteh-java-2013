package ru.fizteh.fivt.students.ermolenko.shell;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Scanner;

public class Shell {

    private State state = new State();

    public Shell(File currentDirectory) {
        currentDirectory = currentDirectory.getAbsoluteFile();
        state.setPath((currentDirectory.toPath()));
    }

    public State getState() {
        return state;
    }

    public void setState(Path inState) {
        state.setPath(inState);
    }

    public void batchState(Shell shell, String[] args) throws IOException {
        StringBuilder tmp = new StringBuilder();

        //слили все слова в одну строку
        for (String arg : args) {
            tmp.append(arg).append(" ");
        }

        //создали массив команд
        String[] command = tmp.toString().split("\\;");

        String cmd = "";
        Execurtor exec = new Execurtor();

        //подаем команды на выполнение
        for (int i = 0; i < command.length - 1; ++i) {

            cmd = command[i].trim();
            try {
                exec.execute(this, cmd);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }

    public void interactiveState() throws IOException {
        Scanner scanner = new Scanner(System.in);
        Execurtor exec = new Execurtor();
        String input;
        String[] cmd;
        while (true) {
            System.out.print(state.getPath().toString() + "$ ");
            cmd = scanner.nextLine().trim().split("\\s*;\\s*");
            try {
                for (String aCmd : cmd) {
                    if (!aCmd.equals("exit")) {
                        exec.execute(this, aCmd);
                    } else {
                        System.exit(0);
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}