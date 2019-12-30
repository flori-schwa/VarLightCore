package me.shawlaf.varlight.persistence;

import me.shawlaf.varlight.util.IntPosition;

public interface ICustomLightSource {

    IntPosition getPosition();

    String getType();

    boolean isMigrated();

    int getCustomLuminance();

}
