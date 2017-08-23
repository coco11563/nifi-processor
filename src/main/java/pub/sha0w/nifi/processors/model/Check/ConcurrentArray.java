package pub.sha0w.nifi.processors.model.Check;

import pub.sha0w.nifi.processors.model.DuliModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static pub.sha0w.nifi.processors.model.Check.DuliCheck.multiCheck;

//一个使用同步锁的线程安全数组
public class ConcurrentArray {
    private final int[] DupWith;
    public ConcurrentArray(int size) {
        DupWith = new int[size];
        for (int i = 0 ; i < DupWith.length ; i ++) {
            DupWith[i] = -1;
        }
    }

    public void concurrentChange(int index, int change) {
        synchronized (DupWith) {
            DupWith[index] = change;
        }
    }

    public int concurrentGet(int index) {
        synchronized (DupWith) {
            return DupWith[index];
        }
    }

    public synchronized int[] concurrentGetArray() {
        return DupWith;
    }

    public boolean concurrentInitAndCheck(int index) {
        synchronized (DupWith) {
            if (DupWith[index] == -1) {
                DupWith[index] = index;
                return true; //haven't been initial
            } else {
                return false; //has been initial
            }
        }
    }

    /**
     *
     * @param index
     * @return false if not initial (dup(index) == -1)
     *          true if initial (dup(index) != -1)
     */
    public boolean concurrentCheckInit(int index) {
        synchronized (DupWith) {
           return DupWith[index] == -1; //false if not initial
        }
    }
    public static ConcurrentArray valueOf(List<HashMap<String, String>> stat, DuliModel duliModel, double range) {
        ConcurrentArray DupWith = new ConcurrentArray(stat.size());
        int size = stat.size();
        int index = 0;
        for (HashMap<String, String> m : stat) {
            long startOne = System.currentTimeMillis();
            if (!DupWith.concurrentInitAndCheck(index)) {
                index ++;
                continue;
            }
            for (int i = index + 1; i < size ; i ++) {
                if (DupWith.concurrentCheckInit(i)) {
                    if (multiCheck(m, stat.get(i),duliModel, range)){
                        //dup
                        DupWith.concurrentChange(i, index);
                    }
                }
                //not dup then continue
            }
            index ++;
            System.out.println(System.currentTimeMillis() - startOne);
        }
        return DupWith;
    }
    public static ConcurrentArray valueOf(List<HashMap<String, String>> stat,ConcurrentArray dupWith,int start, int end, DuliModel duliModel, double range) {
        for (int i1 = start; i1 < end; i1++) {
            Map<String, String> m = stat.get(i1);
            if (!dupWith.concurrentInitAndCheck(i1)) {
                continue;
            }
            for (int i = i1  + 1; i < end; i++) {
                if (dupWith.concurrentCheckInit(i1 )) {
                    if (multiCheck(m, stat.get(i), duliModel, range)) {
                        //dup
                        dupWith.concurrentChange(i, i1 );
                    }
                }
                //not dup then continue
            }
        }
        return dupWith;
    }
}
