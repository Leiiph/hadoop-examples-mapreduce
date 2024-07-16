package com.opstty.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NbTreeRed extends Reducer<Text, NullWritable, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, one);
    }

}
