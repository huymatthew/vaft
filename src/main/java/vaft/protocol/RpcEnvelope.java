package vaft.protocol;

public class RpcEnvelope {
    private MessageType messageType;
    private int senderId;
    private int receiverId;
    private int requestId;
    private Object payload;

    public RpcEnvelope() {}

    public RpcEnvelope(MessageType type, int senderId, Object payload) {
        this.messageType = type;
        this.senderId = senderId;
        this.payload = payload;
    }

    public RpcEnvelope(MessageType type, int senderId, int receiverId, Object payload) {
        this.messageType = type;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.payload = payload;
    }

    public RpcEnvelope(MessageType messageType, int senderId, int receiverId,
                       int requestId, Object payload) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.requestId = requestId;
        this.payload = payload;
    }

    public MessageType getType() {
        return messageType;
    }

    public void setType(MessageType type) {
        this.messageType = type;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public int getReceiverId() { return receiverId; }
    public void setReceiverId(int receiverId) { this.receiverId = receiverId; }
    public int getRequestId() { return requestId; }
    public void setRequestId(int requestId) { this.requestId = requestId; }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }
}
