raft-project/
├── pom.xml
├── src/main/proto/raft.proto
├── src/main/java/
│   ├── network/        (Giai đoạn 0: TCP server/client)
│   ├── grpc/           (Giai đoạn 1: service impl, generated code)
│   ├── core/           (Giai đoạn 2-3: RaftNode, StateMachine, Election, LogReplication)
│   ├── storage/        (Giai đoạn 4: Persistence)
│   └── Main.java
└── src/test/java/      (unit test)