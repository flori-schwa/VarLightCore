package me.shawlaf.varlight.util;

import lombok.experimental.UtilityClass;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

@UtilityClass
public class FileUtil {
    public static String getExtension(File file) {
        String path = file.getAbsolutePath();

        return path.substring(path.lastIndexOf('.'));
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
