package com.recon.common;

import com.recon.common.util.CaseUid;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CaseUidTest {

    @Test
    void sameInputProducesSameUid() {
        String uid1 = CaseUid.of("FILE_ABCDEF123456", 42);
        String uid2 = CaseUid.of("FILE_ABCDEF123456", 42);
        assertEquals(uid1, uid2, "UUID-5 must be deterministic");
    }

    @Test
    void differentLineNoProducesDifferentUid() {
        String uid1 = CaseUid.of("FILE_ABCDEF123456", 1);
        String uid2 = CaseUid.of("FILE_ABCDEF123456", 2);
        assertNotEquals(uid1, uid2);
    }

    @Test
    void differentFileIdProducesDifferentUid() {
        String uid1 = CaseUid.of("FILE_AAAAAAAAAA", 1);
        String uid2 = CaseUid.of("FILE_BBBBBBBBBB", 1);
        assertNotEquals(uid1, uid2);
    }

    @Test
    void uidIsHexStringOf32Chars() {
        String uid = CaseUid.of("FILE_ABCDEF123456", 1);
        assertEquals(36, uid.length(), "UUID with dashes = 36 chars");
        assertTrue(uid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"), "Must be standard UUID format");
    }
}
