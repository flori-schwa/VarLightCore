package me.shawlaf.varlight.test.persistence;

import me.shawlaf.varlight.persistence.BasicCustomLightSource;
import me.shawlaf.varlight.persistence.RegionPersistor;
import me.shawlaf.varlight.persistence.vldb.VLDBFile;
import me.shawlaf.varlight.persistence.vldb.VLDBInputStream;
import me.shawlaf.varlight.persistence.vldb.VLDBOutputStream;
import me.shawlaf.varlight.util.IntPosition;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

public class TestRegionPersistor {

    private File tempDir;

    @BeforeEach
    public void initTempDir(@TempDir File tempDir) {
        this.tempDir = tempDir;
    }

    @Test
    public void testIsNotDirectory() {
        File notDirectory = new File(tempDir, "test");

        try {
            assertTrue(notDirectory.createNewFile());

            assertThrows(IllegalArgumentException.class, () -> new RegionPersistorBasic(notDirectory, 0, 0));
        } catch (IOException e) {
            fail(e);
        }
    }

    @Test
    public void testFileNotExist() {
        class Test {
            public Test(int rx, int rz) throws IOException {
                RegionPersistorBasic persistorBasic = new RegionPersistorBasic(tempDir, rx, rz);

                assertEquals(rx, persistorBasic.regionX);
                assertEquals(rz, persistorBasic.regionZ);

                persistorBasic.file.save();

                File saveFile = new File(tempDir, String.format(VLDBFile.FILE_NAME_FORMAT, rx, rz));

                assertTrue(saveFile.exists());

                try (FileInputStream fis = new FileInputStream(saveFile)) {
                    VLDBInputStream vis = new VLDBInputStream(new GZIPInputStream(fis));

                    vis.readVLDBMagic();

                    assertEquals(rx, vis.readInt32());
                    assertEquals(rz, vis.readInt32());
                    assertEquals(0, vis.readInt16());

                    assertEquals(0, fis.available());
                }
            }
        }

        try {
            new Test(0, 0);
            new Test(0, 1);
            new Test(1, 0);
            new Test(1, 1);
            new Test(-1, -1);
        } catch (IOException e) {
            fail(e);
        }
    }

    @Test
    public void testFileExists() {
        class Test {
            public Test(int rx, int rz) throws IOException {
                File testFile = new File(tempDir, String.format(VLDBFile.FILE_NAME_FORMAT, rx, rz));

                final int x = 32 * 16 * rx + ThreadLocalRandom.current().nextInt(32 * 16);
                final int y = ThreadLocalRandom.current().nextInt(256);
                final int z = 32 * 16 * rz + ThreadLocalRandom.current().nextInt(32 * 16);

                final IntPosition pos = new IntPosition(x, y, z);

                final int light = ThreadLocalRandom.current().nextInt(1, 16);
                final boolean migrated = ThreadLocalRandom.current().nextBoolean();

                try (FileOutputStream fos = new FileOutputStream(testFile)) {
                    GZIPOutputStream gzipOut = new GZIPOutputStream(fos);
                    VLDBOutputStream out = new VLDBOutputStream(gzipOut);

                    out.write(new BasicCustomLightSource[]{
                            new BasicCustomLightSource(pos, light, migrated, "SAND")
                    });

                    gzipOut.flush();
                    gzipOut.close();
                }

                RegionPersistorBasic regionPersistorBasic = new RegionPersistorBasic(tempDir, rx, rz);

                regionPersistorBasic.loadChunk(pos.toChunkCoords());

                BasicCustomLightSource bcls = regionPersistorBasic.getLightSource(pos);

                assertNotNull(bcls);

                assertEquals(pos, bcls.getPosition());
                assertEquals(light, bcls.getCustomLuminance());
                assertEquals(migrated, bcls.isMigrated());
//                assertFalse(regionPersistorBasic.save()); // TODO put in separate test
//
//                regionPersistorBasic.put(new BasicCustomLightSource(pos, light, migrated, "DIRT"));
//
//                regionPersistorBasic.unloadChunk(pos.toChunkCoords());
//                assertTrue(regionPersistorBasic.save());
            }
        }

        try {
            new Test(0, 0);
            new Test(0, 1);
            new Test(1, 0);
            new Test(1, 1);
            new Test(-1, -1);
        } catch (IOException e) {
            fail(e);
        }
    }

    private static class RegionPersistorBasic extends RegionPersistor<BasicCustomLightSource> {
        public RegionPersistorBasic(@NotNull File vldbRoot, int regionX, int regionZ) throws IOException {
            super(vldbRoot, regionX, regionZ, true);
        }

        @Override
        protected BasicCustomLightSource[] createArray(int size) {
            return new BasicCustomLightSource[size];
        }

        @NotNull
        @Override
        protected BasicCustomLightSource[][] createMultiArr(int size) {
            return new BasicCustomLightSource[size][];
        }

        @Override
        protected BasicCustomLightSource createInstance(IntPosition position, int lightLevel, boolean migrated, String material) {
            return new BasicCustomLightSource(position, lightLevel, migrated, material);
        }
    }

}
