
import org.junit.Before;
import org.junit.Test;
import pub.sha0w.nifi.processors.StringUtils.StringNearby;

import java.sql.*;

public class TestStringUtils {
    Connection connection;
    @Before
    public void init() throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        String url="jdbc:mysql://127.0.0.1:3306/orcl_cnic?serverTimezone=UTC&characterEncoding=utf8&useSSL=false&user=root&password=1234";
        connection = DriverManager.getConnection(url);
    }
    @Test
    public void testSimple() {
        long st = System.currentTimeMillis();
        try {
            Statement stmt = connection.createStatement();
            String sql = "select * from export_is_pub4_jnl_full_old";
            ResultSet resultSet = stmt.executeQuery(sql);
            resultSet.beforeFirst();
            while (resultSet.next()) {
                if (mainKeyCompare(resultSet, connection, "Doi", "ARTICLE_NUMBER")) {
                    System.out.println("not duli");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - st);
    }

    /**
     *
     * @param resultSet 传递一个数据库结果集
     * @param connection 传递一个可以用来执行SQL的Connection
     * @param args 需要比对的参数
     * @return TRUE : 该值可能唯一
     *          FALSE : 该值一定不唯一
     * @throws SQLException 数据库出现一些问题
     */
    private boolean mainKeyCompare(ResultSet resultSet, Connection connection, String... args) throws SQLException {
        Statement statement = connection.createStatement();
        StringBuilder querySQL = new StringBuilder("select count(*) from export_is_pub4_jnl_full_old a ") ;
        String temp;
        int flag = 0;
        int len = 0;
        for (String arg : args) {
            if ((temp = resultSet.getString(arg)) != null) {
                if (flag == 0) {
                    querySQL.append("where ");
                    flag ++;
                }
                querySQL.append("a.").append(arg).append(" != null && ").append("a.").append(arg).append(" = \'").append(temp).append("\'");
            } else {
                len ++;
            }
        }
        if (len == args.length) return true;
        System.out.println(querySQL.toString());
        ResultSet rs =  statement.executeQuery(querySQL.toString());
        rs.first();
        return rs.getInt(1) == 0; //attention this should start with 1
    }
}
