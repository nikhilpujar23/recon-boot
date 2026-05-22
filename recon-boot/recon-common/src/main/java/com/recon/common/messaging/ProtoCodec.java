package com.recon.common.messaging;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Thin wrapper around protobuf binary serialization so callers don't repeat try/catch.
 */
public final class ProtoCodec {

    private ProtoCodec() {}

    public static byte[] encode(Message message) {
        return message.toByteArray();
    }

    public static <T extends Message> T decode(byte[] bytes, Parser<T> parser) {
        try {
            return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode protobuf message", e);
        }
    }
}
