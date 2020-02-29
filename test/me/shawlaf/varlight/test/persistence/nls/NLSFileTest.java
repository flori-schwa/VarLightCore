package me.shawlaf.varlight.test.persistence.nls;

import me.shawlaf.varlight.persistence.nls.NLSFile;
import me.shawlaf.varlight.util.IntPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NLSFileTest {

    private byte[] buildTestData(int version, int regionX, int regionZ) {
        byte[] testData = new byte[4 * 4 + 2 * 2 + 2048];

        int i = 0;

        testData[i++] = 0x4E;
        testData[i++] = 0x41;
        testData[i++] = 0x4C;
        testData[i++] = 0x53;

        testData[i++] = (byte) ((version >>> 24) & 0xFF);
        testData[i++] = (byte) ((version >>> 16) & 0xFF);
        testData[i++] = (byte) ((version >>> 8) & 0xFF);
        testData[i++] = (byte) ((version) & 0xFF);

        testData[i++] = (byte) ((regionX >>> 24) & 0xFF);
        testData[i++] = (byte) ((regionX >>> 16) & 0xFF);
        testData[i++] = (byte) ((regionX >>> 8) & 0xFF);
        testData[i++] = (byte) ((regionX) & 0xFF);

        testData[i++] = (byte) ((regionZ >>> 24) & 0xFF);
        testData[i++] = (byte) ((regionZ >>> 16) & 0xFF);
        testData[i++] = (byte) ((regionZ >>> 8) & 0xFF);
        testData[i++] = (byte) ((regionZ) & 0xFF);

        testData[i++] = 0;
        testData[i++] = 0;

        testData[i++] = 0;
        testData[i++] = 1;

        testData[i++] = 0x01;
        testData[i++] = 0x23;
        testData[i++] = 0x45;
        testData[i++] = 0x67;
        testData[i++] = (byte) 0x89;
        testData[i++] = (byte) 0xAB;
        testData[i++] = (byte) 0xCD;
        testData[i++] = (byte) 0xEF;

        Arrays.fill(testData, i, testData.length - 1, (byte) 0);

        return testData;
    }

    private void writeGzipped(File file, byte[] data) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fos)) {
                gzipOutputStream.write(data, 0, data.length);
            }
        }
    }

    @Test
    public void testRead(@TempDir File tempDir) throws IOException {
        byte[] testData = buildTestData(1, 0, 0);

        File file = new File(tempDir, "r.0.0.nls");

        writeGzipped(file, testData);

        NLSFile nlsFile = NLSFile.existingFile(file);

        for (int x = 0; x < 16; ++x) {
            assertEquals(x, nlsFile.getCustomLuminance(new IntPosition(x, 0, 0)));
        }
    }

    @Test
    public void testWrite(@TempDir File tempDir) throws IOException {
        File file = new File(tempDir, "r.0.0.nls");
        NLSFile nlsFile = NLSFile.newFile(file, 0, 0);

        for (int x = 0; x < 16; ++x) {
            nlsFile.setCustomLuminance(new IntPosition(x, 0, 0), x);
        }

        nlsFile.save();

        nlsFile = NLSFile.existingFile(file);

        for (int x = 0; x < 16; ++x) {
            assertEquals(x, nlsFile.getCustomLuminance(new IntPosition(x, 0, 0)));
        }
    }

    @Test
    public void stressTest(@TempDir File tempDir) throws IOException {
        File file = new File(tempDir, "r.0.0.nls");
        NLSFile nlsFile = NLSFile.newFile(file, 0, 0);

        long start = System.currentTimeMillis();
        long lastSplit = start;
        long now;

        System.out.println("[0ms] Writing");

        int i = 1;

        for (int y = 0; y < 256; ++y) {
            for (int z = 0; z < 32 * 16; ++z) {
                for (int x = 0; x < 32 * 16; ++x) {
                    nlsFile.setCustomLuminance(new IntPosition(x, y, z), i);

                    if (++i == 16) {
                        i = 1;
                    }
                }
            }
        }

        now = System.currentTimeMillis();
        System.out.println("[" + (now - start) + "ms] Saving (Writing took: " + (now - lastSplit) + "ms)");
        lastSplit = System.currentTimeMillis();

        nlsFile.save();

        now = System.currentTimeMillis();
        System.out.println("[" + (now - start) + "ms] Reading (Saving took: " + (now - lastSplit) + "ms)");
        lastSplit = now;

        nlsFile = NLSFile.existingFile(file);

        i = 1;

        now = System.currentTimeMillis();
        System.out.println("[" + (now - start) + "ms] Verifying (Reading took: " + (now - lastSplit) + "ms)");

        for (int y = 0; y < 256; ++y) {
            for (int z = 0; z < 32 * 16; ++z) {
                for (int x = 0; x < 32 * 16; ++x) {
                    assertEquals(i, nlsFile.getCustomLuminance(new IntPosition(x, y, z)));

                    if (++i == 16) {
                        i = 1;
                    }
                }
            }
        }
    }

    @Test
    public void testWrongVersion(@TempDir File tempDir) throws IOException {
        byte[] testData = buildTestData(4, 0, 0);

        File file = new File(tempDir, "r.0.0.nls");

        writeGzipped(file, testData);

        assertThrows(IllegalStateException.class, () -> NLSFile.existingFile(file, true));
    }

    @Test
    public void testFileNotExist(@TempDir File tempDir) {
        File file = new File(tempDir, "r.0.0.nls");

        assertThrows(IllegalArgumentException.class, () -> NLSFile.existingFile(file, true));
    }

}
