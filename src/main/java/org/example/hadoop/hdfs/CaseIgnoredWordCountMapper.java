package org.example.hadoop.hdfs;

public class CaseIgnoredWordCountMapper implements Mapper {
    @Override
    public void map(String line, Context context) {
        String[] words = line.toLowerCase().split("\t");
        for (String word : words) {
            Object val = context.read(word);
            if(val == null){
                context.write(word, 1);
            }else{
                context.write(word, (int)val + 1);
            }
        }
    }
}
