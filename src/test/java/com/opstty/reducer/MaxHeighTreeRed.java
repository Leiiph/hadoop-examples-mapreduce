package com.opstty.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxHeighTreeRed extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxHeight = Double.MIN_VALUE;
        for (DoubleWritable val : values) {
            maxHeight = Math.max(maxHeight, val.get());
        }
        context.write(key, new DoubleWritable(maxHeight));
    }
}
