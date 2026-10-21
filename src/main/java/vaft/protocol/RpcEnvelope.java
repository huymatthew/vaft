package vaft.protocol;

public class RpcEnvelope {
    private MessageType messageType;
    private String senderId;
    private String receiverId;
    private String requestId;
    private Object payload;

    public RpcEnvelope() {}

    public RpcEnvelope(MessageType type, String senderId, Object payload) {
        this.messageType = type;
        this.senderId = senderId;
        this.payload = payload;
    }

    public RpcEnvelope(MessageType messageType, String senderId, String receiverId,
                       String requestId, Object payload) {
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

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getReceiverId() { return receiverId; }
    public void setReceiverId(String receiverId) { this.receiverId = receiverId; }
    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }
}
