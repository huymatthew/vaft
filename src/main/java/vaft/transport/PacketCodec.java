package vaft.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import vaft.protocol.RpcEnvelope;

public final class PacketCodec {
    private PacketCodec() {}
    public static void writeFrame(OutputStream output, RpcEnvelope envelope) throws IOException {
        throw new UnsupportedOperationException("TCP framing is not implemented");
    }
    public static RpcEnvelope readFrame(InputStream input) throws IOException {
        throw new UnsupportedOperationException("TCP framing is not implemented");
    }
}
