package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SpeciesMap extends Mapper<Object, Text, Text, NullWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (key.get() == 0) {       // Header line
            return;
        }
        String[] fields = line.split(";");
        String species = fields[3];
        context.write(new Text(species), NullWritable.get());
    }

}