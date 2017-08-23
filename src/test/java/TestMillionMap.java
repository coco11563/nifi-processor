import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TestMillionMap {
    @Test
    public void test() {
        Map<String, String > map = new HashMap<>();
        int index = 0;
        for (int i = 0 ; i < 3000000 ; i ++) {
            while(index == 100000) { System.out.println("done 100000");index = 0 ;}
            map.put(String.valueOf(i),String.valueOf(30));
            index ++;
        }
        long start = System.currentTimeMillis();
        for (int i = 0 ; i < 3000000 ; i ++) {
            map.get(String.valueOf(i));
        }
        System.out.println(System.currentTimeMillis() - start);
    }
    @Test
    public void mapGetNull() {
        Map<String, String > map = new HashMap<>();
        System.out.println(map.get("0"));
        String value = map.get("0");
        System.out.println(Objects.equals(null,map.get("0")));
        System.out.println(Objects.equals(null,value));
    }
    @Test
    //300W -> 1000000(?)
    //30W -> 110703
    //3W -> 10894
    //3K -> 1727
    public void testLocalMillionRedisGet() {
        Jedis redis = new Jedis("localhost", 32768, 40000000);
        long start = System.currentTimeMillis();
        for ( int i = 0 ; i < 300000 ; i ++) {
            redis.hget("1", "PUB_TYPE");
        }
        System.out.println(System.currentTimeMillis() - start);
    }
    @Test
    //300W -> 10000000
    //30W -> 1000000
    //3W -> 1144664
    //3K -> 10000
    public void testRemoteMillionRedisGet() {
        Jedis redis = new Jedis("10.0.82.173", 6379, 40000000);
        long start = System.currentTimeMillis();
        for ( int i = 0 ; i < 30000 ; i ++) {
            redis.hget("1", "PUB_TYPE");
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
