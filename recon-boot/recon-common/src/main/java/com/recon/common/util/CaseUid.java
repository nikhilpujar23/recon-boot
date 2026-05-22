package com.recon.common.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic case identifier: UUID-5(DNS namespace, "fileId:lineNo").
 * Identical algorithm to the Python implementation so IDs are cross-compatible.
 */
public final class CaseUid {

    private static final UUID DNS_NAMESPACE = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    private CaseUid() {}

    public static String of(String fileId, int lineNo) {
        String name = fileId + ":" + lineNo;
        return uuidV5(DNS_NAMESPACE, name).toString();
    }

    private static UUID uuidV5(UUID namespace, String name) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] nsBytes = toBytes(namespace);
        byte[] combined = new byte[nsBytes.length + nameBytes.length];
        System.arraycopy(nsBytes, 0, combined, 0, nsBytes.length);
        System.arraycopy(nameBytes, 0, combined, nsBytes.length, nameBytes.length);

        java.security.MessageDigest sha1;
        try {
            sha1 = java.security.MessageDigest.getInstance("SHA-1");
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hash = sha1.digest(combined);

        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50); // version 5
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80); // variant

        long msb = 0, lsb = 0;
        for (int i = 0; i < 8; i++) msb = (msb << 8) | (hash[i] & 0xff);
        for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (hash[i] & 0xff);
        return new UUID(msb, lsb);
    }

    private static byte[] toBytes(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] buf = new byte[16];
        for (int i = 7; i >= 0; i--) { buf[i] = (byte) (msb & 0xff); msb >>= 8; }
        for (int i = 15; i >= 8; i--) { buf[i] = (byte) (lsb & 0xff); lsb >>= 8; }
        return buf;
    }
}
