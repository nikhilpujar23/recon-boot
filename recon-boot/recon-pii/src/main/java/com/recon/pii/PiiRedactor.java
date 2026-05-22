package com.recon.pii;

import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

/**
 * Applies regex masking to structured string fields (VPA, phone, email).
 * PAN masking is handled by PanVault.tokenize().
 */
@Component
public class PiiRedactor {

    private static final Pattern VPA_PATTERN   = Pattern.compile("[a-z0-9._%+\\-]+@[a-z0-9.\\-]+");
    private static final Pattern PHONE_PATTERN = Pattern.compile("\\b[6-9]\\d{9}\\b");
    private static final Pattern EMAIL_PATTERN = Pattern.compile("[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}");
    private static final Pattern PAN_PATTERN   = Pattern.compile("[A-Z]{5}[0-9]{4}[A-Z]");

    private final HmacTokenizer hmac;

    public PiiRedactor(HmacTokenizer hmac) {
        this.hmac = hmac;
    }

    public String redact(String text) {
        if (text == null) return null;
        text = EMAIL_PATTERN.matcher(text).replaceAll(m -> "EML_" + hmac.sign(m.group()).substring(0, 7));
        text = VPA_PATTERN.matcher(text).replaceAll(m -> "VPA_" + hmac.sign(m.group()).substring(0, 9));
        text = PHONE_PATTERN.matcher(text).replaceAll(m -> "PHN_" + hmac.sign(m.group()).substring(0, 8));
        text = PAN_PATTERN.matcher(text).replaceAll(m -> "PAN_" + hmac.sign(m.group()).substring(0, 8));
        return text;
    }

    public boolean containsPii(String text) {
        if (text == null) return false;
        return EMAIL_PATTERN.matcher(text).find()
                || PHONE_PATTERN.matcher(text).find()
                || PAN_PATTERN.matcher(text).find();
    }
}
