package me.shawlaf.varlight.persistence;

import me.shawlaf.varlight.persistence.vldb.VLDBFile;
import me.shawlaf.varlight.util.ChunkCoords;
import me.shawlaf.varlight.util.IntPosition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.*;

public abstract class RegionPersistor<L extends ICustomLightSource> {

    private static final int REGION_SIZE = 32;
    private static final int CHUNK_SIZE = 16 * 16 * 256;

    public final int regionX, regionZ;

    public final VLDBFile<L> file;

    private final Object chunkLock = new Object();

    private final L[][] chunkCache = createMultiArr(REGION_SIZE * REGION_SIZE);
    private final int[] chunkSizes = new int[REGION_SIZE * REGION_SIZE];
    private final List<ChunkCoords> dirtyChunks = new ArrayList<>(REGION_SIZE * REGION_SIZE);

    public RegionPersistor(@NotNull File vldbRoot, int regionX, int regionZ, boolean deflated) throws IOException {
        Objects.requireNonNull(vldbRoot);

        if (!vldbRoot.exists()) {
            if (!vldbRoot.mkdir()) {
                throw new LightPersistFailedException("Could not create directory \"" + vldbRoot.getAbsolutePath() + "\"");
            }
        }

        if (!vldbRoot.isDirectory()) {
            throw new IllegalArgumentException(String.format("\"%s\" is not a directory!", vldbRoot.getAbsolutePath()));
        }

        this.regionX = regionX;
        this.regionZ = regionZ;

        File vldbFile = new File(vldbRoot, String.format(VLDBFile.FILE_NAME_FORMAT, regionX, regionZ));

        if (!vldbFile.exists()) {
            this.file = new VLDBFile<L>(vldbFile, regionX, regionZ, deflated) {
                @NotNull
                @Override
                protected L[] createArray(int size) {
                    return RegionPersistor.this.createArray(size);
                }

                @NotNull
                @Override
                protected L createInstance(IntPosition position, int lightLevel, boolean migrated, String material) {
                    return RegionPersistor.this.createInstance(position, lightLevel, migrated, material);
                }
            };
        } else {
            this.file = new VLDBFile<L>(vldbFile, deflated) {
                @NotNull
                @Override
                protected L[] createArray(int size) {
                    return RegionPersistor.this.createArray(size);
                }

                @NotNull
                @Override
                protected L createInstance(IntPosition position, int lightLevel, boolean migrated, String material) {
                    return RegionPersistor.this.createInstance(position, lightLevel, migrated, material);
                }
            };
        }
    }

    public void markDirty(IntPosition position) {
        markDirty(position.toChunkCoords());
    }

    public void markDirty(ChunkCoords chunkCoords) {
        synchronized (chunkLock) {
            dirtyChunks.add(chunkCoords);
        }
    }

    public void loadChunk(@NotNull ChunkCoords chunkCoords) throws IOException {
        Objects.requireNonNull(chunkCoords);

        final int chunkIndex = chunkIndex(chunkCoords);

        synchronized (chunkLock) {
            L[] chunk;

            synchronized (file) {
                chunk = file.readChunk(chunkCoords);
            }

            L[] chunkArray = createArray(CHUNK_SIZE);

            for (L lightsource : chunk) {
                chunkArray[indexOf(lightsource.getPosition())] = lightsource;
            }

            chunkSizes[chunkIndex] = chunk.length;
            chunkCache[chunkIndex] = chunkArray;
        }
    }

    public boolean isChunkLoaded(@NotNull ChunkCoords chunkCoords) {
        Objects.requireNonNull(chunkCoords);

        synchronized (chunkLock) {
            return chunkCache[chunkIndex(chunkCoords)] != null;
        }
    }

    public void unloadChunk(@NotNull ChunkCoords chunkCoords) throws IOException {
        Objects.requireNonNull(chunkCoords);

        final int chunkIndex = chunkIndex(chunkCoords);

        synchronized (chunkLock) {
            L[] toUnload = chunkCache[chunkIndex];

            if (toUnload == null) { // There was no mapping for the chunk
                return;
            }

            flushChunk(chunkCoords, getNonNullFromChunk(chunkCoords));

            chunkSizes[chunkIndex] = 0;
            chunkCache[chunkIndex] = null;
        }
    }

    @NotNull
    public List<L> getCache(@NotNull ChunkCoords chunkCoords) {
        List<L> chunk;

        synchronized (chunkLock) {
            L[] chunkArray = chunkCache[chunkIndex(chunkCoords)];

            if (chunkArray == null) {
                chunk = new ArrayList<>();
            } else {
                chunk = Arrays.asList(chunkArray);
            }
        }

        return Collections.unmodifiableList(chunk);
    }

    @Nullable
    public L getLightSource(@NotNull IntPosition position) throws IOException {
        Objects.requireNonNull(position);

        final ChunkCoords chunkCoords = position.toChunkCoords();
        final int chunkIndex = chunkIndex(chunkCoords);

        synchronized (chunkLock) {
            if (chunkCache[chunkIndex] == null) {
                loadChunk(chunkCoords);
            }

            return chunkCache[chunkIndex][indexOf(position)];
        }
    }

    public void put(@NotNull L lightSource) throws IOException {
        Objects.requireNonNull(lightSource);

        final ChunkCoords chunkCoords = lightSource.getPosition().toChunkCoords();
        final int chunkIndex = chunkIndex(chunkCoords);

        synchronized (chunkLock) {
            if (chunkCache[chunkIndex] == null) {
                loadChunk(chunkCoords);
            }

            putInternal(lightSource);
        }
    }

    @Deprecated
    public void removeLightSource(@NotNull IntPosition position) throws IOException {
        Objects.requireNonNull(position);

        final ChunkCoords chunkCoords = position.toChunkCoords();
        final int chunkIndex = chunkIndex(chunkCoords);
        final int index = indexOf(position);

        synchronized (chunkLock) {
            if (chunkCache[chunkIndex] == null) {
                loadChunk(chunkCoords);
            }

            L[] chunkArray = chunkCache[chunkIndex];

            if (chunkArray[index] != null) {
                chunkArray[index] = null;
                --chunkSizes[chunkIndex(chunkCoords)];

                markDirty(chunkCoords);
            }
        }
    }

    public void flushAll() throws IOException {
        synchronized (chunkLock) {
            synchronized (file) {
                for (ChunkCoords key : dirtyChunks.toArray(new ChunkCoords[0])) {
                    flushChunk(key);
                }
            }
        }
    }

    public List<ChunkCoords> getAffectedChunks() {
        synchronized (file) {
            return new ArrayList<>(file.getOffsetTable().keySet());
        }
    }

    public List<L> loadAll() throws IOException {
        synchronized (file) {
            synchronized (chunkLock) {
                int cx, cz;

                for (int z = 0; z < REGION_SIZE; ++z) {
                    for (int x = 0; x < REGION_SIZE; ++x) {
                        if (chunkCache[chunkIndex(cx = regionX + x, cz = regionZ + z)] != null) {
                            flushChunk(new ChunkCoords(cx, cz));
                        }
                    }
                }

                return file.readAll();
            }
        }
    }

    public boolean save() throws IOException {
        synchronized (file) {
            return file.save();
        }
    }

    private void flushChunk(ChunkCoords chunkCoords, Collection<L> lightData) throws IOException {
        final int chunkIndex = chunkIndex(chunkCoords);

        synchronized (chunkLock) {
            if (!dirtyChunks.contains(chunkCoords)) {
                return;
            }

            synchronized (file) {
                if (lightData.size() == 0) {
                    if (file.hasChunkData(chunkCoords)) {
                        file.removeChunk(chunkCoords);
                    }

                    chunkCache[chunkIndex] = null;
                    chunkSizes[chunkIndex] = 0;
                } else if (!file.hasChunkData(chunkCoords)) {
                    file.insertChunk(lightData.toArray(createArray(0)));
                } else {
                    file.editChunk(chunkCoords, lightData.toArray(createArray(0)));
                }
            }

            dirtyChunks.remove(chunkCoords);
        }
    }

    private void flushChunk(ChunkCoords chunkCoords) throws IOException {
        synchronized (chunkLock) {
            flushChunk(chunkCoords, getNonNullFromChunk(chunkCoords));
        }
    }

    private void putInternal(L lightSource) {
        Objects.requireNonNull(lightSource);

        final ChunkCoords chunkCoords = lightSource.getPosition().toChunkCoords();
        final int chunkIndex = chunkIndex(chunkCoords);
        final int index = indexOf(lightSource.getPosition());

        synchronized (chunkLock) {
            L[] chunkArray = chunkCache[chunkIndex];

            if (chunkArray == null) {
                throw new IllegalArgumentException("No Data present for chunk");
            }

            L removed = chunkArray[index];
            chunkArray[index] = null;

            if (lightSource.getCustomLuminance() > 0) { // New or modified
                chunkArray[index] = lightSource;

                if (removed == null) {
                    ++chunkSizes[chunkIndex]; // One new light source added
                }

                // When a light source was modified, aka removed != null, then the amount of Light sources stays the same

                markDirty(chunkCoords);
            } else { // Removed, or no-op
                if (removed != null) {
                    markDirty(chunkCoords);
                    --chunkSizes[chunkIndex]; // One Light source was removed
                }
            }
        }
    }

    private Collection<L> getNonNullFromChunk(ChunkCoords chunkCoords) {
        final int chunkIndex = chunkIndex(chunkCoords);

        synchronized (chunkLock) {
            int chunkSize = chunkSizes[chunkIndex];

            List<L> list = new ArrayList<>(chunkSize);
            L[] rawArr = chunkCache[chunkIndex];

            if (rawArr == null || rawArr.length == 0) {
                return list; // Will have size 0
            }

            int added = 0;

            for (L l : rawArr) {
                if (l == null) {
                    continue;
                }

                list.add(l);

                if (++added == chunkSize) {
                    break;
                }
            }

            return list;
        }
    }

    private int indexOf(IntPosition position) {
        return indexOf(position.getChunkRelativeX(), position.y, position.getChunkRelativeZ());
    }

    private int indexOf(int x, int y, int z) {
        return (y << 8) | (z << 4) | x;
    }

    private int chunkIndex(ChunkCoords chunkCoords) {
        return chunkIndex(chunkCoords.getRegionRelativeX(), chunkCoords.getRegionRelativeZ());
    }

    private int chunkIndex(int cx, int cz) {
        return cz << 5 | cx;
    }

    @NotNull
    protected abstract L[] createArray(int size);

    @NotNull
    protected abstract L[][] createMultiArr(int size);

    @NotNull
    protected abstract L createInstance(IntPosition position, int lightLevel, boolean migrated, String material);

    public void unload() {
        Arrays.fill(chunkCache, null);
        Arrays.fill(chunkSizes, 0);

        dirtyChunks.clear();

        file.unload();
    }
}