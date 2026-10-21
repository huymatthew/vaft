package vaft.transport;

import java.io.*;
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
