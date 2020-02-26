package me.shawlaf.varlight.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ParameterRange {

    public static void assertInRange(String paramName, int value, int minIncl, int maxIncl) {
        if (value < minIncl) {
            throw new IllegalArgumentException("Parameter " + paramName + " out range: must be >= " + minIncl);
        }

        if (value > maxIncl) {
            throw new IllegalArgumentException("Parameter " + paramName + "out of range: must be <= " + maxIncl);
        }
    }

}
