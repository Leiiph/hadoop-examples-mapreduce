package com.opstty.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HeighSortRed extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val); // Directly write the height and species
        }
    }
}
