package vaft.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class StateMachineTest {

    private StateMachine stateMachine;

    @BeforeEach
    public void setUp() {
        stateMachine = new StateMachine();
    }

    @Test
    public void testSetAndGet() {
        // Cú pháp mới: SET key value
        String result1 = stateMachine.apply("SET user:1 Tuan");
        assertEquals("OK", result1);
        assertEquals("Tuan", stateMachine.get("user:1"));

        // Cú pháp tương thích ngược: SET key=value
        String result2 = stateMachine.apply("SET user:2=Duc");
        assertEquals("OK", result2);
        assertEquals("Duc", stateMachine.get("user:2"));

        // Cú pháp có khoảng trắng: SET key = value
        String result3 = stateMachine.apply("SET user:3 = Nam");
        assertEquals("OK", result3);
        assertEquals("Nam", stateMachine.get("user:3"));
    }

    @Test
    public void testDel() {
        stateMachine.apply("SET token abcxyz");
        assertEquals("abcxyz", stateMachine.get("token"));

        // Xóa key thành công
        String delRes = stateMachine.apply("DEL token");
        assertEquals("OK", delRes);
        assertNull(stateMachine.get("token"));

        // Xóa key không tồn tại
        String delNonExist = stateMachine.apply("DEL not_found_key");
        assertTrue(delNonExist.startsWith("ERROR:"));
    }

    @Test
    public void testCas() {
        stateMachine.apply("SET count 100");

        // CAS thành công khi expectedValue khớp (100 -> 200)
        String casSuccess = stateMachine.apply("CAS count 100 200");
        assertEquals("OK", casSuccess);
        assertEquals("200", stateMachine.get("count"));

        // CAS thất bại khi expectedValue không khớp (mong muốn 999 nhưng hiện tại là 200)
        String casFailed = stateMachine.apply("CAS count 999 300");
        assertTrue(casFailed.startsWith("ERROR: CAS_FAILED"));
        assertEquals("200", stateMachine.get("count")); // Giá trị không bị đổi
    }

    @Test
    public void testExists() {
        stateMachine.apply("SET active true");

        assertEquals("TRUE", stateMachine.apply("EXISTS active"));
        assertEquals("FALSE", stateMachine.apply("EXISTS inactive"));
        assertTrue(stateMachine.exists("active"));
        assertFalse(stateMachine.exists("inactive"));
    }

    @Test
    public void testKeysAndScan() {
        stateMachine.apply("SET user:1 Alice");
        stateMachine.apply("SET user:2 Bob");
        stateMachine.apply("SET order:100 ItemA");

        // 1. Kiểm tra lệnh KEYS (lấy toàn bộ)
        String allKeysRes = stateMachine.apply("KEYS");
        assertTrue(allKeysRes.contains("user:1"));
        assertTrue(allKeysRes.contains("user:2"));
        assertTrue(allKeysRes.contains("order:100"));

        // 2. Kiểm tra lệnh KEYS với pattern tiền tố "user:" hoặc "user:*"
        String userKeysRes = stateMachine.apply("KEYS user:");
        assertTrue(userKeysRes.contains("user:1"));
        assertTrue(userKeysRes.contains("user:2"));
        assertFalse(userKeysRes.contains("order:100"));

        // 3. Kiểm tra qua hàm Java scanKeys()
        Set<String> scannedUserKeys = stateMachine.scanKeys("user:*");
        assertEquals(2, scannedUserKeys.size());
        assertTrue(scannedUserKeys.contains("user:1"));
        assertTrue(scannedUserKeys.contains("user:2"));

        // 4. Kiểm tra khi không tìm thấy key nào khớp
        String emptyRes = stateMachine.apply("KEYS product:*");
        assertEquals("(empty)", emptyRes);
    }

    @Test
    public void testSnapshotRestore() {
        stateMachine.apply("SET k1 v1");
        stateMachine.apply("SET k2 v2");
        stateMachine.apply("SET k3 v3");

        Map<String, String> snapshot = stateMachine.getSnapshotData();
        assertEquals(3, snapshot.size());
        assertEquals("v1", snapshot.get("k1"));

        // Xóa sạch stateMachine
        stateMachine.clear();
        assertEquals(0, stateMachine.size());

        // Khôi phục lại từ snapshot
        stateMachine.restoreSnapshotData(snapshot);
        assertEquals(3, stateMachine.size());
        assertEquals("v1", stateMachine.get("k1"));
        assertEquals("v2", stateMachine.get("k2"));
        assertEquals("v3", stateMachine.get("k3"));
    }
}
