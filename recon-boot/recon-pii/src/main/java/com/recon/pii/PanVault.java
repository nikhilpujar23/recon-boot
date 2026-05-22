package com.recon.pii;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AES-256-GCM vault for PAN tokenization.
 * token → ciphertext+iv+tag stored in pan_vault table (via PanVaultRepository).
 * Deterministic: same PAN always produces the same token within a run (in-memory dedup).
 */
@Component
public class PanVault {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;

    private final SecretKey aesKey;
    private final HmacTokenizer hmac;
    // in-memory cache for determinism within a run
    private final ConcurrentHashMap<String, String> tokenCache = new ConcurrentHashMap<>();

    public PanVault(HmacTokenizer hmac, com.recon.common.config.AppConfig config) {
        this.hmac = hmac;
        byte[] keyBytes = Base64.getDecoder().decode(config.pii().panAesKey());
        this.aesKey = new SecretKeySpec(keyBytes, "AES");
    }

    /** Tokenize a PAN. Returns a stable token of the form PAN_BIN_HMAC_LAST4. */
    public String tokenize(String pan) {
        return tokenCache.computeIfAbsent(pan, this::doTokenize);
    }

    private String doTokenize(String pan) {
        String bin   = pan.length() >= 6 ? pan.substring(0, 6) : pan;
        String last4 = pan.length() >= 4 ? pan.substring(pan.length() - 4) : pan;
        String hmacHex = hmac.sign(pan).substring(0, 8);
        return "PAN_" + bin + "_" + hmacHex + "_" + last4;
    }

    /** Encrypt plaintext PAN for storage in pan_vault (returns base64 envelope). */
    public String encrypt(String pan) throws Exception {
        byte[] iv = new byte[GCM_IV_LENGTH];
        new SecureRandom().nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", "BC");
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
        byte[] ciphertext = cipher.doFinal(pan.getBytes());

        // envelope: iv || ciphertext (tag appended by GCM)
        byte[] envelope = new byte[iv.length + ciphertext.length];
        System.arraycopy(iv, 0, envelope, 0, iv.length);
        System.arraycopy(ciphertext, 0, envelope, iv.length, ciphertext.length);
        return Base64.getEncoder().encodeToString(envelope);
    }

    /** Decrypt a stored envelope back to PAN. */
    public String decrypt(String base64Envelope) throws Exception {
        byte[] envelope = Base64.getDecoder().decode(base64Envelope);
        byte[] iv         = Arrays.copyOfRange(envelope, 0, GCM_IV_LENGTH);
        byte[] ciphertext = Arrays.copyOfRange(envelope, GCM_IV_LENGTH, envelope.length);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", "BC");
        cipher.init(Cipher.DECRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_LENGTH, iv));
        return new String(cipher.doFinal(ciphertext));
    }
}
