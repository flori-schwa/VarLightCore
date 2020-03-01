package me.shawlaf.varlight.persistence.nls;

import lombok.experimental.UtilityClass;

@UtilityClass
public class NLSUtil {

    public static final int CURRENT_VERSION = 1;

    public static final int SIZEOF_INT16 = 2;
    public static final int SIZEOF_INT32 = 4;

    public static final int NLS_MAGIC = 0x4E_41_4C_53;

    public static final int SIZEOF_NLS_MAGIC = SIZEOF_INT32;

}
