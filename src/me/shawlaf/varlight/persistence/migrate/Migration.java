package me.shawlaf.varlight.persistence.migrate;

public interface Migration<M> {

    boolean migrate(M toMigrate);

    default String getName() {
        return getClass().getSimpleName();
    }

}
