package vaft.transport;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import vaft.protocol.AppendEntriesRequest;
import vaft.protocol.AppendEntriesResponse;
import vaft.protocol.LogEntry;
import vaft.protocol.MessageType;
import vaft.protocol.RequestVoteRequest;
import vaft.protocol.RequestVoteResponse;
import vaft.protocol.RpcEnvelope;

public class PacketCodec {
    private static final short MAGIC = 0x5641; // "VA" - VAFT Protocol

    public static void writePacket(DataOutputStream out, RpcEnvelope envelope) throws IOException {
        // 1. Ghi Header chung
        out.writeShort(MAGIC);
        out.writeByte(envelope.getType().toCode());
        out.writeInt(envelope.getSenderId());
        out.writeInt(envelope.getReceiverId());

        // 2. Ghi Payload tùy theo loại bản tin
        switch (envelope.getType()) {
            case REQUEST_VOTE_REQ -> {
                RequestVoteRequest req = (RequestVoteRequest) envelope.getPayload();
                out.writeInt(req.getTerm());
                out.writeInt(req.getCandidateId());
                out.writeInt(req.getLastLogIndex());
                out.writeInt(req.getLastLogTerm());
            }
            case REQUEST_VOTE_RES -> {
                RequestVoteResponse res = (RequestVoteResponse) envelope.getPayload();
                out.writeInt(res.getTerm());
                out.writeBoolean(res.isVoteGranted());
            }
            case APPEND_ENTRIES_REQ -> {
                AppendEntriesRequest req = (AppendEntriesRequest) envelope.getPayload();
                out.writeInt(req.getTerm());
                out.writeInt(req.getLeaderId());
                out.writeInt(req.getPrevLogIndex());
                out.writeInt(req.getPrevLogTerm());
                out.writeInt(req.getLeaderCommit());

                List<LogEntry> entries = req.getEntries();
                int size = (entries == null) ? 0 : entries.size();
                out.writeInt(size);
                for (int i = 0; i < size; i++) {
                    LogEntry entry = entries.get(i);
                    out.writeInt(entry.index());
                    out.writeInt(entry.term());
                    out.writeUTF(entry.command());
                }
            }
            case APPEND_ENTRIES_RES -> {
                AppendEntriesResponse res = (AppendEntriesResponse) envelope.getPayload();
                out.writeInt(res.getTerm());
                out.writeBoolean(res.isSuccess());
                out.writeInt(res.getMatchIndex());
            }
        }

        // Bắt buộc flush để đẩy toàn bộ buffer xuống TCP Socket ngay lập tức
        out.flush();
    }

    public static RpcEnvelope readPacket(DataInputStream in) throws IOException {
        // 1. Đọc và kiểm tra Header
        short magic = in.readShort();
        if (magic != MAGIC) {
            throw new IOException("Gói tin không hợp lệ (Sai Magic Number): " + magic);
        }

        MessageType type = MessageType.fromCode(in.readByte());
        int senderId = in.readInt();
        int receiverId = in.readInt();

        // 2. Đọc Payload theo đúng thứ tự đã ghi
        Object payload = switch (type) {
            case REQUEST_VOTE_REQ -> new RequestVoteRequest(
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    in.readInt()
            );
            case REQUEST_VOTE_RES -> new RequestVoteResponse(
                    in.readInt(),
                    in.readBoolean()
            );
            case APPEND_ENTRIES_REQ -> {
                int term = in.readInt();
                int leaderId = in.readInt();
                int prevLogIndex = in.readInt();
                int prevLogTerm = in.readInt();
                int leaderCommit = in.readInt();

                int size = in.readInt();
                List<LogEntry> entries = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    entries.add(new LogEntry(in.readInt(), in.readInt(), in.readUTF()));
                }
                yield new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
            }
            case APPEND_ENTRIES_RES -> new AppendEntriesResponse(
                    in.readInt(),
                    in.readBoolean(),
                    in.readInt(),
                    in.readInt()
            );
        };

        return new RpcEnvelope(type, senderId, receiverId, payload);
    }
}