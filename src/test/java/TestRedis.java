//
//import org.junit.Before;
//import org.junit.Test;
//
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.redis.core.*;
//import org.springframework.test.context.ContextConfiguration;
//import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//import pub.sha0w.nifi.processors.model.DuliModel;
//import pub.sha0w.nifi.processors.model.Key;
//
//import javax.annotation.Resource;
//import java.sql.*;
//import java.util.Map;
//
//@ContextConfiguration({"classpath:spring-redis-context.xml"})
//@RunWith(SpringJUnit4ClassRunner.class)
//public class TestRedis {
//    private int index = 0;
//
//    @Autowired
//    private RedisTemplate<String, String> template;
//
//    private HashOperations<String, String, String> hashOps;
//
//    @Before
//    public void init() throws SQLException, ClassNotFoundException {
//        Class.forName("com.mysql.jdbc.Driver");
//        String url="jdbc:mysql://127.0.0.1:3306/orcl_cnic?serverTimezone=UTC&characterEncoding=utf8&useSSL=false&user=root&password=1234";
//        Connection connection = DriverManager.getConnection(url);
//        Statement stmt = connection.createStatement();
//        String sql = "select * from export_is_pub4_jnl_full_old";
//        ResultSet rs = stmt.executeQuery(sql);
//        ResultSetMetaData resultSetMetaData = rs.getMetaData();
//        int columnCount = resultSetMetaData.getColumnCount();
//        String[] argsList = new String[columnCount];
//        for (int i = 0 ; i < columnCount ; i ++) {
//            argsList[i] = resultSetMetaData.getColumnName(i + 1);
//        }
//        rs.beforeFirst();
//        Map<String, String> tempRow;
//        System.out.println("START INIT REAL REDIS");
//        hashOps = template.opsForHash();
//        while (rs.next()) {
//            for (String arg : argsList) {
//                hashOps.put(String.valueOf(index) ,arg, rs.getString(arg));
//            }
//            hashOps.put(String.valueOf(index), "dupWith","null");
//            index++;
//        }
//        System.out.println("INIT DONE");
//    }
//    @Test
//    public void test() {
//        String argsList = "ISSUE_BY&REWARD_NUMBER;ZH_NAME|EN_NAME,PUBLISH_YEAR,ISSUED_BY,REWARD_TYPE,REWARD_RANK";
//        DuliModel duliModel = DuliModel.valueOf(argsList);
//        Map<String, String> previous = hashOps.entries("0");
//        Key MK = duliModel.getMainKey();
//        Key VK = duliModel.getVagueKey();
//        //MK PROCESS
//        String temp;
//        String multiTemp_1;
//        String multiTemp_2;
//        long pastTime = System.currentTimeMillis();
//        if (previous.get("dupWith").equals("null")) {
//            previous.put("dupWith", "1");
//        }
//        for (int j = 0 ; j < index ; j ++) {
//            for (String mk : MK.getMain()) {
//                if (mk.contains("&&")) { //联合查询
//                    String[] argTemp = mk.split("&&");
//                    if ((multiTemp_1 = previous.get(argTemp[0]))!= null && (multiTemp_2 = previous.get(argTemp[0])) != null) {
//                        if (multiTemp_1.equals(hashOps.entries(String.valueOf(j)).get(argTemp[0])) && multiTemp_2.equals(hashOps.entries(String.valueOf(j)).get(argTemp[1]))) { //dup
//                            System.out.println("DUP!");
//                            break;
//                        }
//                    }
//                } else { //非联合查询
//                    if ((temp = previous.get(mk)) != null) {
//                        if (temp.equals(hashOps.entries(String.valueOf(j)).get(temp))) { //dup
//                            System.out.println("DUP!");
//                            break;
//                        }
//                    }
//                }
//            }
//            testSupplement(j, 1);
//            System.out.println("NOT DUP!");
//        }
//        System.out.println(System.currentTimeMillis() - pastTime);
//        //VK PROCESS
//    }
//
//    private void testSupplement(int index, int dup) {
//        hashOps.entries(String.valueOf(index)).put("dupWith", String.valueOf(dup));
//    }
//}
