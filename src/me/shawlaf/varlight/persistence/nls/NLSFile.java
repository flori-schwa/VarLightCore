package me.shawlaf.varlight.persistence.nls;

import me.shawlaf.varlight.persistence.nls.io.NLSInputStream;
import me.shawlaf.varlight.persistence.nls.io.NLSOutputStream;
import me.shawlaf.varlight.util.ChunkCoords;
import me.shawlaf.varlight.util.FileUtil;
import me.shawlaf.varlight.util.IntPosition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

public class NLSFile {
    public static String FILE_NAME_FORMAT = "r.%d.%d.nls";

    public final File file;

    private final Object lock = new Object();
    private final int regionX, regionZ;
    private final boolean deflate;

    private boolean modified;

    private ChunkLightStorage[] chunks = new ChunkLightStorage[32 * 32];

    public static NLSFile newFile(@NotNull File file, int regionX, int regionZ) {
        return new NLSFile(file, regionX, regionZ, true);
    }

    public static NLSFile newFile(@NotNull File file, int regionX, int regionZ, boolean deflate) {
        return new NLSFile(file, regionX, regionZ, deflate);
    }

    public static NLSFile existingFile(@NotNull File file) throws IOException {
        return new NLSFile(file, true);
    }

    public static NLSFile existingFile(@NotNull File file, boolean deflate) throws IOException {
        return new NLSFile(file, deflate);
    }

    private NLSFile(@NotNull File file, int regionX, int regionZ, boolean deflate) {
        Objects.requireNonNull(file);

        if (file.exists()) {
            throw new IllegalArgumentException("File already exists!");
        }

        this.file = file;
        this.deflate = deflate;

        this.regionX = regionX;
        this.regionZ = regionZ;
    }

    private NLSFile(@NotNull File file, boolean deflate) throws IOException {
        Objects.requireNonNull(file);

        if (!file.exists()) {
            throw new IllegalArgumentException("File does not exist");
        }

        this.file = file;
        this.deflate = deflate;

        synchronized (lock) {
            try (NLSInputStream in = openNLSFile(file)) {
                in.verifyNLSMagic();

                int version = in.readInt32();

                if (version != NLSUtil.CURRENT_VERSION) {
                    throw new IllegalStateException("Expected NLS Version " + NLSUtil.CURRENT_VERSION + ", got " + version);
                }

                this.regionX = in.readInt32();
                this.regionZ = in.readInt32();

                try {
                    while (true) {
                        int position = in.readInt16();
                        chunks[position] = ChunkLightStorage.read(position, regionX, regionZ, in);
                    }
                } catch (EOFException e) {
                    // Ignore
                }
            }
        }
    }

    public int getCustomLuminance(IntPosition position) {
        synchronized (lock) {
            ChunkLightStorage chunk = getChunk(position.toChunkCoords());

            if (chunk == null) {
                return 0;
            }

            return chunk.getCustomLuminance(position);
        }
    }

    public void setCustomLuminance(IntPosition position, int value) {
        ChunkCoords chunkCoords = position.toChunkCoords();
        int index = chunkIndex(chunkCoords);

        synchronized (lock) {
            ChunkLightStorage chunk = getOrCreateChunk(index, () -> chunkCoords);

            chunk.setCustomLuminance(position, value);

            if (value == 0) {
                if (chunk.getMask() == 0) {
                    chunks[index] = null;
                }
            }

            modified = true;
        }
    }

    @Nullable
    private ChunkLightStorage getChunk(ChunkCoords chunkCoords) {
        return chunks[chunkIndex(chunkCoords)];
    }

    @NotNull
    private ChunkLightStorage getOrCreateChunk(ChunkCoords chunkCoords) {
        return getOrCreateChunk(chunkIndex(chunkCoords), () -> chunkCoords);
    }

    @NotNull
    private ChunkLightStorage getOrCreateChunk(int index, Supplier<ChunkCoords> chunkCoordsSupplier) {
        if (chunks[index] == null) {
            return (chunks[index] = new ChunkLightStorage(chunkCoordsSupplier.get()));
        }

        return chunks[index];
    }

    private NLSInputStream openNLSFile(File file) throws IOException {
        return new NLSInputStream(FileUtil.openStreamInflate(file));
    }

    private int chunkIndex(ChunkCoords chunkCoords) {
        return chunkIndex(chunkCoords.getRegionRelativeX(), chunkCoords.getRegionRelativeZ());
    }

    private int chunkIndex(int cx, int cz) {
        return cz << 5 | cx;
    }

    public boolean save() throws IOException {
        synchronized (lock) {
            if (!modified) {
                return false;
            }

            try (NLSOutputStream out = new NLSOutputStream(deflate ? new GZIPOutputStream(new FileOutputStream(file)) : new FileOutputStream(file))) {
                out.writeHeader(regionX, regionZ);
                ChunkLightStorage cls;

                for (int i = 0; i < chunks.length; ++i) {
                    cls = chunks[i];

                    if (cls == null) {
                        continue;
                    }

                    out.writeInt16(i);
                    cls.writeData(out);
                }
            }

            modified = false;
        }

        return true;
    }

}
