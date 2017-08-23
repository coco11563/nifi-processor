package pub.sha0w.nifi.processors.model.Check;

import pub.sha0w.nifi.processors.StringUtils.StringNearby;
import pub.sha0w.nifi.processors.model.DuliModel;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DuliCheck {

    private ConcurrentArray DupWith;

    /**
     *
     * @param mapA compare mother
     * @param mapB compare son
     * @param duliModel dup model
     * @param range vague compare tolerance
     * @return true if dup
     *          false if not dup
     */
    public static boolean multiCheck(Map<String, String> mapA, Map<String, String> mapB, DuliModel duliModel, double range) {
        boolean isDup;
        if (mainKeyCompare(mapA, mapB, duliModel)) {
            isDup = true;
        } else {
            isDup = vagueKeyCompare(mapA, mapB, duliModel, range);
        }
        return isDup;
    }
    private static boolean mainKeyCompare(Map<String, String> mapA, Map<String, String> mapB, DuliModel duliModel) {
        String[] key = duliModel.getMainKey().getMain();
        String[] multi;
        for (String s : key) {
            if (s.contains("&")) {
                multi = s.split("&");
                boolean flag = true;
                for (String at : multi) {
                    flag = flag & keyEqual(mapA.get(at), mapB.get(at));
                }
                if (flag) return true;
            } else {
                String keyA = mapA.get(s.toUpperCase());
                String keyB = mapB.get(s.toUpperCase());
                if (keyEqual(keyA, keyB)) return true;
            }
        }
        return false;
    }

    /**
     * 关键Key匹配
     * @param a 字符串A
     * @param b 字符串B
     * @return true if a == b
     */
    private static boolean keyEqual(String a, String b) {
        return a != null && !Objects.equals(a, "") && Objects.equals(a, b);
    }

    private static boolean vagueKeyCompare(Map<String, String> mapA, Map<String, String> mapB, DuliModel duliModel, double range) {
        String[] key = duliModel.getVagueKey().getMain();
        String[] compareA = mapGetStringArray(key, mapA);
        String[] compareB = mapGetStringArray(key, mapB);
        return  (compare(compareA, compareB) < range); //true is dup
    }

    /**
     * 根据一串字符串队列得到Map中对应的字符串队列
     * @param keySet 需要的关键字
     * @param map 需要检索的Map
     * @return 结果字符串队列
     */
    private static String[] mapGetStringArray(String[] keySet, Map<String, String> map) {
        String[] ret = new String[keySet.length];
        int index = 0;
        String temps;
        for (String s : keySet) {
            temps = map.get(s);
            ret[index] = (temps == null) ?  "" : temps;
            index ++;
        }
        return ret;
    }

    /**
     * 这俩重复了，需要在之后的步骤中合并
     * @param compareIndex 需要修改的索引
     * @param index 修改的值h
     */
    private void dupToSupplement(String index, String compareIndex) {
    }

    /**
     * 对比单个字符串对
     * @param a 字符串A
     * @param b 字符串B
     * @return 需要移动的步数
     */
    private static int compareString(String a, String b) {
//        return new StringNearby(a,b).getDistance();
        return 1;
    }

    /**
     * 得到两个字符串相加的长度
     * @param a 字符串A
     * @param b 字符串B
     * @return 长度
     */
    private static int lenString(String a, String b) {
        if (a == null) {
            if (b == null) return 0;
            else return b.length();
        } else {
            if (b == null) return a.length();
            else return b.length() + a.length();
        }
    }

    /**
     * 输入两个字符串数组，得到这些数组的加权相差度
     * @param a 字符串A
     * @param b 字符串B
     * @return Double < 1.0
     */
    private static double compare(String[] a, String[] b) {
        if (a.length != b.length) return -1;
        if (a.length == 0) return 0;
        int alllen = 0;
        int allCom = 0;
        for(int i = 0 ; i < a.length ; i ++) {
            alllen += lenString(a[i], b[i]);
            allCom += compareString(a[i], b[i]);
        }
        return (allCom + 0.0) / alllen;
    }

    public void printMap(Map<String, String> map) {
        System.out.println();
        for (String key : map.keySet()) {
            System.out.print(key + " : "  + map.get(key) + "    ");
        }
    }



}
