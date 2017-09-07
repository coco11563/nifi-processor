//import org.junit.Before;
//import org.junit.Test;
//import pub.sha0w.nifi.processors.StringUtils.StringNearby;
//import pub.sha0w.nifi.processors.model.DuliModel;
//import pub.sha0w.nifi.processors.model.Key;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.Pipeline;
//
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Objects;
//
//public class TestRedisDup {
//    private int dup = 0;
//    private double range = 0.001;
//    private Jedis redis;
//    //key 是一条记录， value是该条记录重复的记录
//    private Map<String, String> localCache = new HashMap<>();
//    @Before
//    public void initConn() {
////        try {
//////            new TestInitRedis().init();
////        } catch (SQLException | ClassNotFoundException e) {
////            e.printStackTrace();
////        }
//        redis = new Jedis("localhost", 32768, 40000000);
//        redis.select(0);
//    }
//    @Test
//    public void redisGetNullTest() {
//        Map map = redis.hgetAll("0");
//        System.out.println(map.size());
//    }
//
//    @Test
//    public void pipTest() {
//        Pipeline p = redis.pipelined();
//        int index = 0;
//        Map<String, String> temp;
//        Map<String, String> compare;
//        DuliModel duliModel = new DuliModel("DOI,ARTICLE_NO;TITLE_ZH,TITLE_EN,PUB_YEAR,JOURNAL,VOLUME,SERIES,START_PAGE,END_PAGE");
//        long all = System.currentTimeMillis();
//        while ((temp = redis.hgetAll(String.valueOf(index))).size() != 0) { //normal case is 31 , only nil will hgetSize = 0
//            String in = String.valueOf(index);
//            long one = System.currentTimeMillis();
//            String dupwith = localCache.get(in);
//            if (Objects.equals(dupwith, null)) {
//                dupwith = String.valueOf(index);
//                localCache.put("dupWith", dupwith);
//            }
//            int compareIndex = index + 1;
//            while ((compare = redis.hgetAll(String.valueOf(compareIndex))).size() != 0) {
//                if (!(Objects.equals(compare.get("dupWith"), "-1"))) {continue;} //this stuff is dup with somebody(base on swap theory)
//                if (mainKeyCompare(temp, compare,duliModel)) { //dup
//                    dupToSupplement(String.valueOf(compareIndex), dupwith);
//                    System.out.println("this map:");
//                    printMap(temp);
//                    System.out.println("dup map:");
//                    printMap(compare);
//                    index ++;
//                    redis.hmset(String.valueOf(index), temp);
//                    break;
//                } else {
//                    if (vagueKeyCompare(temp, compare, duliModel)) { //dup
//                        System.out.println("this map:");
//                        printMap(temp);
//                        System.out.println("dup map:");
//                        printMap(compare);
//                        dupToSupplement(String.valueOf(compareIndex), dupwith);
//                        redis.hmset(String.valueOf(index), temp);
//                        index ++;
//                        break;
//                    } else { //not dup
//                        redis.hmset(String.valueOf(index), temp);
//                        compareIndex += 1;
//                    }
//                }
//            }
//            //all not dup
//            index ++;
//            System.out.println("done one in " + ((System.currentTimeMillis() - one) / 1000) + "s");
//            System.out.println("dup time ： " + dup);
//            System.out.println("all stuff : " + index);
//        }
//        System.out.println("all done in " + ((System.currentTimeMillis() - all) / 1000) + "s");
//    }
//
//    public boolean mainKeyCompare(Map<String, String> mapA, Map<String, String> mapB, DuliModel duliModel) {
//        String[] key = duliModel.getMainKey().getMain();
//        String[] multi;
//        for (String s : key) {
//            if (s.contains("&")) {
//                multi = s.split("&");
//                boolean flag = true;
//                for (String at : multi) {
//                    flag = flag & keyEqual(mapA.get(at), mapB.get(at));
//                }
//                if (flag) return true;
//            } else {
//                String keyA = mapA.get(s.toUpperCase());
//                String keyB = mapB.get(s.toUpperCase());
//                if (keyEqual(mapA.get(s), mapB.get(s))) return true;
//            }
//        }
//        return false;
//    }
//
//    /**
//     * 关键Key匹配
//     * @param a 字符串A
//     * @param b 字符串B
//     * @return true if a == b
//     */
//    private boolean keyEqual(String a, String b) {
//        return a != null && !Objects.equals(a, "") && Objects.equals(a, b);
//    }
//
//    private boolean vagueKeyCompare(Map<String, String> mapA, Map<String, String> mapB, DuliModel duliModel) {
//        String[] key = duliModel.getVagueKey().getMain();
//        String[] compareA = mapGetStringArray(key, mapA);
//        String[] compareB = mapGetStringArray(key, mapB);
//        return  (compare(compareA, compareB) < range);
//    }
//
//    /**
//     * 根据一串字符串队列得到Map中对应的字符串队列
//     * @param keySet 需要的关键字
//     * @param map 需要检索的Map
//     * @return 结果字符串队列
//     */
//    public String[] mapGetStringArray(String[] keySet, Map<String, String> map) {
//        String[] ret = new String[keySet.length];
//        int index = 0;
//        String temps;
//        for (String s : keySet) {
//            temps = map.get(s);
//            ret[index] = (temps == null) ?  "" : temps;
//            index ++;
//        }
//        return ret;
//    }
//
//    /**
//     * 这俩重复了，需要在之后的步骤中合并
//     * @param compareIndex 需要修改的索引
//     * @param index 修改的值h
//     */
//    private void dupToSupplement(String index, String compareIndex) {
//        dup ++;
//        localCache.put(compareIndex, index);
//    }
//
//    /**
//     * 对比单个字符串对
//     * @param a 字符串A
//     * @param b 字符串B
//     * @return 需要移动的步数
//     */
//    private int compareString(String a, String b) {
//        return new StringNearby(a,b).getDistance();
//    }
//
//    /**
//     * 得到两个字符串相加的长度
//     * @param a 字符串A
//     * @param b 字符串B
//     * @return 长度
//     */
//    private int lenString(String a, String b) {
//        if (a == null) {
//            if (b == null) return 0;
//            else return b.length();
//        } else {
//            if (b == null) return a.length();
//            else return b.length() + a.length();
//        }
//    }
//
//    /**
//     * 输入两个字符串数组，得到这些数组的加权相差度
//     * @param a 字符串A
//     * @param b 字符串B
//     * @return Double < 1.0
//     */
//    private double compare(String[] a, String[] b) {
//        if (a.length != b.length) return -1;
//        if (a.length == 0) return 0;
//        int alllen = 0;
//        int allCom = 0;
//        for(int i = 0 ; i < a.length ; i ++) {
//            alllen += lenString(a[i], b[i]);
//            allCom += compareString(a[i], b[i]);
//        }
//        return (allCom + 0.0) / alllen;
//    }
//
//    public void printMap(Map<String, String> map) {
//        System.out.println();
//        for (String key : map.keySet()) {
//            System.out.print(key + " : "  + map.get(key) + "    ");
//        }
//    }
//    @Test
//    public void testCompareString() {
//        String[] a = new String[]{"Huang, Huai-Xiang; Li, You-Quan; Gan, Jing-Yu; Chen, Yan; Zhang, Fu-Chun","Physical Review B"};
//        String[] b = new String[]{"Yang, Hua; Zhu, Guangshan; Zhang, Daliang; Xu, Diou; Qiu, Shilun","Microporous and Mesoporous Materials"};
//        System.out.println(compare(a, b));
//    }
//}
