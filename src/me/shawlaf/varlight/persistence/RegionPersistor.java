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

    public final int regionX, regionZ;

    public final VLDBFile<L> file;

    private final Object chunkLock = new Object();

    private final Map<ChunkCoords, Map<ChunkOffsetPosition, L>> chunkCache = new HashMap<>();
    private final Set<ChunkCoords> dirtyChunks = new HashSet<>();

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

        synchronized (chunkLock) {
            L[] chunk;

            synchronized (file) {
                chunk = file.readChunk(chunkCoords);
            }

            Map<ChunkOffsetPosition, L> chunkMap = new HashMap<>();

            for (L lightsource : chunk) {
                chunkMap.put(new ChunkOffsetPosition(lightsource.getPosition()), lightsource);
            }

            chunkCache.put(chunkCoords, chunkMap);
        }
    }

    public boolean isChunkLoaded(@NotNull ChunkCoords chunkCoords) {
        Objects.requireNonNull(chunkCoords);

        synchronized (chunkLock) {
            return chunkCache.containsKey(chunkCoords);
        }
    }

    public void unloadChunk(@NotNull ChunkCoords chunkCoords) throws IOException {
        Objects.requireNonNull(chunkCoords);

        synchronized (chunkLock) {
            Map<ChunkOffsetPosition, L> toUnload = chunkCache.remove(chunkCoords);

            if (toUnload == null) { // There was no mapping for the chunk
                return;
            }

            flushChunk(chunkCoords, toUnload.values());
        }
    }

    @NotNull
    public List<L> getCache(@NotNull ChunkCoords chunkCoords) {
        List<L> chunk;

        synchronized (chunkLock) {
            Map<ChunkOffsetPosition, L> chunkMap = chunkCache.get(chunkCoords);

            if (chunkMap == null) {
                chunk = new ArrayList<>();
            } else {
                chunk = new ArrayList<>(chunkMap.values());
            }
        }

        return Collections.unmodifiableList(chunk);
    }

    @Nullable
    public L getLightSource(@NotNull IntPosition position) throws IOException {
        Objects.requireNonNull(position);

        ChunkCoords chunkCoords = position.toChunkCoords();

        synchronized (chunkLock) {
            if (!chunkCache.containsKey(chunkCoords)) {
                loadChunk(chunkCoords);
            }

            return chunkCache.get(chunkCoords).get(new ChunkOffsetPosition(position));
        }
    }

    public void put(@NotNull L lightSource) throws IOException {
        Objects.requireNonNull(lightSource);

        ChunkCoords chunkCoords = lightSource.getPosition().toChunkCoords();

        synchronized (chunkLock) {
            if (!chunkCache.containsKey(chunkCoords)) {
                loadChunk(chunkCoords);
            }

            putInternal(lightSource);
        }
    }

    @Deprecated
    public void removeLightSource(@NotNull IntPosition position) throws IOException {
        Objects.requireNonNull(position);

        ChunkCoords chunkCoords = position.toChunkCoords();

        synchronized (chunkLock) {
            if (!chunkCache.containsKey(chunkCoords)) {
                loadChunk(chunkCoords);
            }

            if (chunkCache.get(chunkCoords).remove(new ChunkOffsetPosition(position)) != null) {
//                Map<ChunkOffsetPosition, L> chunk = chunkCache.get(chunkCoords);
//
//                if (chunk.size() == 0) {
//                    file.removeChunk(chunkCoords);
//                } else {
//                    file.editChunk(chunkCoords, chunk.values().toArray(createArray(0)));
//                }
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
                for (ChunkCoords chunkCoords : chunkCache.keySet()) {
                    flushChunk(chunkCoords);
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
        synchronized (chunkLock) {
            if (!dirtyChunks.contains(chunkCoords)) {
                return;
            }

            synchronized (file) {
                if (lightData.size() == 0) {
                    if (file.hasChunkData(chunkCoords)) {
                        file.removeChunk(chunkCoords);
                    }

                    chunkCache.remove(chunkCoords);
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
            flushChunk(chunkCoords, chunkCache.get(chunkCoords).values());
        }
    }

    private void putInternal(L lightSource) {
        Objects.requireNonNull(lightSource);

        ChunkCoords chunkCoords = lightSource.getPosition().toChunkCoords();
        ChunkOffsetPosition offsetPosition = new ChunkOffsetPosition(lightSource.getPosition());

        synchronized (chunkLock) {
            Map<ChunkOffsetPosition, L> map = chunkCache.get(chunkCoords);

            if (map == null) {
                throw new IllegalArgumentException("No Data present for chunk");
            }

            L removed = map.remove(offsetPosition);

            if (lightSource.getCustomLuminance() > 0) {
                map.put(offsetPosition, lightSource);

                markDirty(chunkCoords);
            } else {
                if (removed != null) {
                    markDirty(chunkCoords);
                }
            }
        }
    }

    @NotNull
    protected abstract L[] createArray(int size);

    @NotNull
    protected abstract L createInstance(IntPosition position, int lightLevel, boolean migrated, String material);

    public void unload() {
        chunkCache.clear();
        dirtyChunks.clear();

        file.unload();
    }

    private static class ChunkOffsetPosition {
        public final int x, y, z;

        public ChunkOffsetPosition(IntPosition position) {
            this(position.x & 0xF, position.y, position.z & 0xF);
        }

        public ChunkOffsetPosition(int x, int y, int z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChunkOffsetPosition that = (ChunkOffsetPosition) o;
            return x == that.x &&
                    y == that.y &&
                    z == that.z;
        }

        @Override
        public int hashCode() {
            return (x << 12) | (y << 4) | z;
        }
    }
}