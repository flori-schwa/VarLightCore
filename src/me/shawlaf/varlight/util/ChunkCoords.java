package me.shawlaf.varlight.util;

import java.util.Objects;

public class ChunkCoords {

    public static final ChunkCoords ORIGIN = new ChunkCoords(0, 0);

    public final int x, z;

    public ChunkCoords(int x, int z) {
        this.x = x;
        this.z = z;
    }

    public int getRegionX() {
        return x >> 5;
    }

    public int getRegionRelativeX() {
        return MathUtil.modulo(x, 32);
    }

    public int getRegionZ() {
        return z >> 5;
    }

    public int getRegionRelativeZ() {
        return MathUtil.modulo(z, 32);
    }

    public IntPosition getRelative(int dx, int dy, int dz) {
        ParameterRange.assertInRange("dx", dx, 0, 15);
        ParameterRange.assertInRange("dy", dy, 0, 255);
        ParameterRange.assertInRange("dz", dz, 0, 15);

        return new IntPosition(this.x * 16 + dx, dy, this.z * 16 + dz);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkCoords that = (ChunkCoords) o;
        return x == that.x &&
                z == that.z;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, z);
    }

    @Override
    public String toString() {
        return "ChunkCoords{" +
                "x=" + x +
                ", z=" + z +
                '}';
    }

    public String toShortString() {
        return String.format("[%d, %d]", x, z);
    }

    public RegionCoords toRegionCoords() {
        return new RegionCoords(getRegionX(), getRegionZ());
    }

    public ChunkSectionPosition toChunkSectionPosition(int y) {
        return new ChunkSectionPosition(this, y);
    }
}
