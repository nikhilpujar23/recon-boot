package com.recon.pii;

import com.recon.common.config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PiiRedactorTest {

    private PiiRedactor redactor;

    @BeforeEach
    void setUp() {
        // Use a fixed 32-byte base64 key for tests
        AppConfig.Pii piiConfig = new AppConfig.Pii(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",  // 32-byte HMAC key (padded)
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="   // 32-byte AES key (padded)
        );
        AppConfig config = new AppConfig(
                new AppConfig.Agent("haiku", "sonnet", 6, 30, true, 0),
                new AppConfig.Rules(1),
                piiConfig,
                new AppConfig.Api("test-token", 60),
                new AppConfig.Retry(1.0, 3, 5.0, 25.0)
        );
        HmacTokenizer hmac = new HmacTokenizer(config);
        redactor = new PiiRedactor(hmac);
    }

    @Test
    void redactsEmail() {
        String result = redactor.redact("Contact us at user@example.com for help");
        assertFalse(result.contains("user@example.com"), "Email should be masked");
        assertTrue(result.contains("EML_"), "Should contain EML_ token");
    }

    @Test
    void redactsPhone() {
        String result = redactor.redact("Call 9876543210 for support");
        assertFalse(result.contains("9876543210"), "Phone should be masked");
        assertTrue(result.contains("PHN_"), "Should contain PHN_ token");
    }

    @Test
    void redactsPan() {
        String result = redactor.redact("PAN is ABCDE1234F");
        assertFalse(result.contains("ABCDE1234F"), "PAN should be masked");
        assertTrue(result.contains("PAN_"), "Should contain PAN_ token");
    }

    @Test
    void nullInputReturnsNull() {
        assertNull(redactor.redact(null));
    }

    @Test
    void cleanTextPassesThrough() {
        String text = "Transaction amount: 5000 paise";
        assertEquals(text, redactor.redact(text));
    }

    @Test
    void containsPiiDetectsEmail() {
        assertTrue(redactor.containsPii("user@example.com"));
    }

    @Test
    void containsPiiReturnsFalseForCleanText() {
        assertFalse(redactor.containsPii("no sensitive data here"));
    }

    @Test
    void sameInputProducesSameToken() {
        String r1 = redactor.redact("user@example.com");
        String r2 = redactor.redact("user@example.com");
        assertEquals(r1, r2, "Tokenization must be deterministic");
    }
}
