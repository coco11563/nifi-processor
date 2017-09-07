//import org.junit.Before;
//import org.junit.Test;
//import pub.sha0w.nifi.processors.model.Check.ConcurrentArray;
//import pub.sha0w.nifi.processors.model.DuliModel;
//
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class TestMemDup {
//    List<HashMap<String, String>> stat = new ArrayList<>();
//
//    DuliModel duliModel = new DuliModel("DOI,ARTICLE_NUMBER;TITLE_ZH,TITLE_EN,PUB_YEAR,JOURNAL,VOLUME,START_PAGE,END_PAGE");
//
//    @Before
//    //1000 -> 22479
//    public void init() throws ClassNotFoundException, SQLException {
//        Class.forName("com.mysql.jdbc.Driver");
//        String url="jdbc:mysql://127.0.0.1:3306/orcl_cnic?serverTimezone=UTC&characterEncoding=utf8&useSSL=false&user=root&password=1234";
//        Connection connection = DriverManager.getConnection(url);
//        Statement stmt = connection.createStatement();
//        String sql = "select " + duliModel.getArgs() + "  from export_is_pub4_jnl_full_old";
//        ResultSet rs = stmt.executeQuery(sql);
//        ResultSetMetaData resultSetMetaData = rs.getMetaData();
//        int columnCount = resultSetMetaData.getColumnCount();
//        String[] argsList = new String[columnCount];
//        for (int i = 0 ; i < columnCount ; i ++) {
//            argsList[i] = resultSetMetaData.getColumnName(i + 1);
//        }
//        rs.beforeFirst();
//        HashMap<String, String> tempRow ;
//        System.out.println("START INIT MAP");
//        String value;
//        while (rs.next()) {
//            tempRow = new HashMap<>();
//            for (String arg : argsList) {
//                value = rs.getString(arg);
//                if (value == null) {
//                    value = "";
//                }
//                tempRow.put(arg, value);
//            }
//           stat.add(tempRow);
//        }
//        System.out.println("init done");
//    }
//
//    @Test
//    public void test() {
//        long start = System.currentTimeMillis();
//        ConcurrentArray concurrentArray = ConcurrentArray.valueOf(stat, duliModel, -1 );
//        System.out.println(System.currentTimeMillis() - start);
//        for (int i : concurrentArray.concurrentGetArray()) {
//            System.out.print(i + ",");
//        }
//    }
//}
