package com.recon.pii;

import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.HexFormat;

/**
 * HMAC-SHA256 signing used for deterministic PAN token generation.
 */
@Component
public class HmacTokenizer {

    private final byte[] keyBytes;

    public HmacTokenizer(com.recon.common.config.AppConfig config) {
        this.keyBytes = Base64.getDecoder().decode(config.pii().panHmacKey());
    }

    public String sign(String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
            byte[] raw = mac.doFinal(value.getBytes());
            return HexFormat.of().formatHex(raw);
        } catch (Exception e) {
            throw new RuntimeException("HMAC signing failed", e);
        }
    }
}
