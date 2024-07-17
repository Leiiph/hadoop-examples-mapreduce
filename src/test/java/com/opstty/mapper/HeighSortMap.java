package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HeighSortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (key.get() == 0) { // Header line
            return;
        }
        String[] fields = line.split(";");
        double height = Double.parseDouble(fields[6]); // Assuming "HAUTEUR" is the 7th column
        context.write(new DoubleWritable(height), new Text(fields[3])); // Output height as key and species as value
    }
}
