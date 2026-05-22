package com.recon.rag;

/** Utility for pgvector string literal formatting. */
final class VectorUtils {

    private VectorUtils() {}

    /** Converts a float[] to a pgvector literal, e.g. "[0.1,0.2,0.3]". */
    static String toLiteral(float[] vector) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(vector[i]);
        }
        return sb.append(']').toString();
    }
}
