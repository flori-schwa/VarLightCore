package me.shawlaf.varlight.persistence;

import lombok.Getter;
import me.shawlaf.varlight.util.IntPosition;

import java.util.Objects;

public class BasicCustomLightSource implements ICustomLightSource {
    @Getter private final IntPosition position;
    @Getter private final String type;
    @Getter private final int customLuminance;
    @Getter private final boolean migrated;

    public BasicCustomLightSource(IntPosition position, int customLuminance, boolean migrated, String type) {
        this.position = position;
        this.type = type;
        this.customLuminance = customLuminance;
        this.migrated = migrated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BasicCustomLightSource that = (BasicCustomLightSource) o;
        return customLuminance == that.customLuminance &&
                migrated == that.migrated &&
                position.equals(that.position) &&
                type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, type, customLuminance, migrated);
    }

    @Override
    public String toString() {
        return "BasicStoredLightSource{" +
                "position=" + position +
                ", type='" + type + '\'' +
                ", emittingLight=" + customLuminance +
                ", migrated=" + migrated +
                '}';
    }
}