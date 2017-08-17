package pub.sha0w.nifi.processors.model;

public class Key {
    private String[] main;
    private Key(String[] in) {
        main = in;
    }
    static Key valueOf(String in) {
        return new Key(split(in));
    }
    private static String[] split(String in) {
        return in.split(",");
    }

    public String[] getMain() {
        return main;
    }
}
