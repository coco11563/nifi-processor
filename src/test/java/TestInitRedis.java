//import org.junit.Before;
//import org.junit.Test;
//import pub.sha0w.nifi.processors.model.DuliModel;
//import pub.sha0w.nifi.processors.model.Key;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.Pipeline;
//
//import java.sql.*;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//
//public class TestInitRedis {
//    private int index = 0;
//    private Jedis redis;
//    @Test
//    public void init() throws SQLException, ClassNotFoundException {
//        Class.forName("com.mysql.jdbc.Driver");
//        String url="jdbc:mysql://127.0.0.1:3306/orcl_cnic?serverTimezone=UTC&characterEncoding=utf8&useSSL=false&user=root&password=1234";
//        Connection connection = DriverManager.getConnection(url);
//        Statement stmt = connection.createStatement();
//        String sql = "select * from export_is_pub4_jnl_full_old limit 2000";
//        ResultSet rs = stmt.executeQuery(sql);
//        ResultSetMetaData resultSetMetaData = rs.getMetaData();
//        int columnCount = resultSetMetaData.getColumnCount();
//        String[] argsList = new String[columnCount];
//        for (int i = 0 ; i < columnCount ; i ++) {
//            argsList[i] = resultSetMetaData.getColumnName(i + 1);
//        }
//        rs.beforeFirst();
//        Map<String, String> tempRow = new HashMap<>();
//        System.out.println("START INIT REAL REDIS");
//        redis = new Jedis("localhost", 32768, 40000000);
//        redis.select(0);
//        redis.flushDB();
//        Pipeline p = redis.pipelined();
//        String value;
//        int syncTimes = 1;
//        while (rs.next()) {
//            tempRow.clear();
//            for (String arg : argsList) {
//                value = rs.getString(arg);
//                if (value == null) {
//                    value = "";
//                }
//                tempRow.put(arg, value);
//            }
//            tempRow.put("dupWith","-1");
//            p.hmset(String.valueOf(index), tempRow);
//            index++;
//            if (index > 10000 * syncTimes) {
//                syncTimes ++;
//                System.out.println(syncTimes);
//                p.sync();
//                p = redis.pipelined();
//            }
//        }
//        p.sync();
//        System.out.println("INIT DONE");
//    }
//}
