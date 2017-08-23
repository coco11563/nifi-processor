package pub.sha0w.nifi.processors.StringUtils;

/**
 * 两种不同的计算方法，都使用了字符串距离编辑算法（Levenshtein Distance）
 */
public class StringNearby {
    private char[] str1;
    private char[] str2;
    private int distance;

    /**
     * 当flag为true时会执行消耗内存大一些的递归方法
     * @param a 比对字符串A
     * @param b 比对字符串B
     * @param flag 使用方法
     */
    public StringNearby(String a, String b, boolean flag) {
        if (flag) {
            str1 = a.toCharArray();
            str2 = b.toCharArray();
            distance = caculateDistance();
        } else {
            str1 = a.toCharArray();
            str2 = b.toCharArray();
            distance = caculateDistance_1();
        }

    }
    public StringNearby(String a, String b) {
        str1 = a.toCharArray();
        str2 = b.toCharArray();
        distance = caculateDistance_1();
    }
    private int caculateDistance() {
        return caculate(str1, str2, 0, str1.length - 1, 0, str2.length - 1);
    }

    private int caculateDistance_1() {
        return caculate(str1, str2);
    }
    public int getDistance() {
        return distance;
    }

    private int caculate(char[] a1, char[] a2, int beginA, int endA, int beginB, int endB) {
        if (beginA > endA) {//如果比较过程，其中一个字符串已经结束
            if (beginB > endB) {//如果另一个字符串也结束
                return 0;
            }
            return endB - beginB + 1;//如果另一个字符串还没结束，那么需要加上剩下的字符个数
        }
        if (beginB > endB) {
            if (beginA > endA) {
                return 0;
            }
            return endA - beginA + 1;
        }
        if (a1[beginA] == a2[beginB]) {
            return caculate(a1, a2, beginA + 1 , endA, beginB + 1, endB);
        }
        int i = caculate(a1, a2, beginA + 1, endA, beginB, endB); // a 删除 1
        int j = caculate(a1, a2, beginA, endA, beginB + 1, endB);
        int k = caculate(a1, a2, beginA + 1, endA, beginB + 1, endB);
        //返回这三种比较方式中，改变最小次数值
        return minOfThree(i, j, k) + 1;
    }

    /**
     * str1或str2的长度为0返回另一个字符串的长度。 if(str1.length==0) return str2.length; if(str2.length==0) return str1.length;
     * 初始化(n+1)*(m+1)的矩阵d，并让第一行和列的值从0开始增长。
     * 扫描两字符串（n*m级的），如果：str1[i] == str2[j]，用temp记录它，为0。否则temp记为1。然后在矩阵d[i,j]赋于d[i-1,j]+1 、d[i,j-1]+1、d[i-1,j-1]+temp三者的最小值。
     * 扫描完后，返回矩阵的最后一个值d[n][m]即是它们的距离。
     * 计算相似度公式：1-它们的距离/两个字符串长度的最大值。
     * @param a1 需要对比的字符串A
     * @param a2 需要对比的字符串B
     * @return 距离
     */
    private int caculate(char[] a1, char[] a2) {
        int la = a1.length;
        int lb = a2.length;
        if (la == 0) return lb;
        if (lb == 0) return la;
        int[][] map = new int[la][lb];
        for (int i = 0 ; i < la ; i ++) {
            map[i][0] = i;
        }
        for (int i = 0 ; i < lb ; i++) {
            map[0][i] = i;
        }
        int temp;
        for (int i = 1; i < la; i++)
        {
            char ch1 = a1[i - 1];
            for (int j = 1; j < lb; j++)
            {
                char ch2 = a2[j - 1];
                if (ch1 == ch2)
                {
                    temp = 0;
                }
                else
                {
                    temp = 1;
                }
                map[i][j] = minOfThree(map[i - 1][j] + 1, map[i][j - 1] + 1, map[i - 1][j - 1] + temp);
            }
        }
        return map[la - 1][lb - 1];
    }
    private int minOfThree(int a, int b, int c) {
        return a < b ? (a < c ? a : c) : (b < c ? b : c);
    }

}
