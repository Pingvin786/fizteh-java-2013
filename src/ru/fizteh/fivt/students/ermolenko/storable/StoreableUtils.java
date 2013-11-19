package ru.fizteh.fivt.students.ermolenko.storable;

import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.students.ermolenko.filemap.FileMapUtils;
import ru.fizteh.fivt.students.ermolenko.multifilehashmap.MultiFileHashMapUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreableUtils {

    public static Class<?> convertStringToClass(String inString) throws IOException {

        return StoreableEnum.getClassByName(inString);
    }

    private static String readType(DataInputStream inDataStream) throws IOException {

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        byte b = inDataStream.readByte();
        int length = 0;
        while (b != ' ' && inDataStream.available() > 0) {
            byteOutputStream.write(b);
            b = inDataStream.readByte();
            length++;
            if (length > 1024 * 1024) {
                throw new IOException("signature.tsv is too big");
            }
        }
        if (length == 0) {
            throw new IOException("signature.tsv has wrong format");
        }
        return byteOutputStream.toString(StandardCharsets.UTF_8.toString());
    }

    public static void readSignature(File inFile, List<Class<?>> inColumnTypes) throws IOException {

        if (!inFile.exists()) {
            throw new IOException("no signature.tsv");
        }
        if (inFile.length() == 0) {
            throw new IOException("signature.tsv is empty");
        }

        InputStream is = new FileInputStream(inFile);
        BufferedInputStream bis = new BufferedInputStream(is, 4096);
        DataInputStream dis = new DataInputStream(bis);
        try {

            int position = 0;
            while (position != inFile.length()) {
                String type = readType(dis);
                position += type.getBytes(StandardCharsets.UTF_8).length + 1;
                Class<?> classType = convertStringToClass(type);
                inColumnTypes.add(classType);
            }
        } finally {
            FileMapUtils.closeStream(dis);
        }
    }

    public static void read(File inFile, StoreableTable table, Map<String, Storeable> currentMap,
                            StoreableTableProvider inProvider) throws IOException {

        Map<String, String> stringMap = new HashMap<String, String>();
        MultiFileHashMapUtils.read(inFile, stringMap);
        for (String key : stringMap.keySet()) {
            try {
                MyStoreable value = inProvider.deserialize(table, stringMap.get(key));
                currentMap.put(key, value);
            } catch (ParseException e) {
                throw new IOException("read error", e);
            }
        }
    }

    private static String convertClassToString(Class<?> type) throws IOException {

        return StoreableEnum.getNameByClass(type);
    }

    public static void writeSignature(File directory, List<Class<?>> columnTypes) throws IOException {

        File signatureFile = new File(directory, "signature.tsv");
        signatureFile.createNewFile();
        OutputStream os = new FileOutputStream(signatureFile);
        BufferedOutputStream bos = new BufferedOutputStream(os, 4096);
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            for (Class<?> type : columnTypes) {
                if (type == null) {
                    throw new IllegalArgumentException("wrong column type");
                }
                String typeString = convertClassToString(type);
                dos.write(typeString.getBytes(StandardCharsets.UTF_8));
                dos.write(' ');
            }
        } finally {
            FileMapUtils.closeStream(dos);
        }
    }

    public static void write(File file, StoreableTable table, HashMap<String, Storeable> storeableMap,
                             StoreableTableProvider tableProvider) throws IOException {

        Map<String, String> stringMap = new HashMap<String, String>();
        for (String key : storeableMap.keySet()) {
            stringMap.put(key, tableProvider.serialize(table, storeableMap.get(key)));
        }
        MultiFileHashMapUtils.write(file, stringMap);
    }
}