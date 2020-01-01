package me.shawlaf.varlight.util;

import lombok.experimental.UtilityClass;

import java.io.*;
import java.util.zip.GZIPInputStream;

@UtilityClass
public class FileUtil {
    public static String getExtension(File file) {
        String path = file.getAbsolutePath();

        return path.substring(path.lastIndexOf('.'));
    }

    public static byte[] readFileFullyInflate(File file) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (FileInputStream fis = new FileInputStream(file)) {
            InputStream in;

            if (isDeflated(file)) {
                in = new GZIPInputStream(fis);
            } else {
                in = fis;
            }

            byte[] buffer = new byte[1024];
            int read = 0;

            while ((read = in.read(buffer, 0, buffer.length)) > 0) {
                baos.write(buffer, 0, read);
            }

            in.close();
        }

        return baos.toByteArray();
    }

    public static boolean isDeflated(File file) throws IOException {
        boolean deflated = false;

        try (FileInputStream fis = new FileInputStream(file)) {
            DataInputStream dataInputStream = new DataInputStream(fis);

            int lsb = dataInputStream.readUnsignedByte();
            int msb = dataInputStream.readUnsignedByte();

            int read = (msb << 8) | lsb;

            if (read == GZIPInputStream.GZIP_MAGIC) {

                read = dataInputStream.readByte();

                if (read == 0x08) {
                    deflated = true;
                }
            }

            dataInputStream.close();
        }

        return deflated;
    }

}
