public class ClickRecord {
    private String host;
    private String query;
    private long freq;
    private final String splitter = "<SPLITTER>";

    public ClickRecord(String line) {
        String[] split = line.split(splitter);
        host = split[0];
        query = split[1];
        freq = Long.parseLong(split[2]);
    }

    public String getHost(){
        return host;
    }

    public String getQuery(){
        return query;
    }

    public long getFreq(){
        return freq;
    }

    public ClickRecord(){
        host = "";
        query = "";
        freq = 0;
    }

    public void set(String host, String query, long freq){
        this.host = host;
        this.query = query;
        this.freq = freq;
    }

    public String toString(){ return host + splitter + query + splitter + Long.toString(freq);    }



}
