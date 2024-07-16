package com.opstty.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistrictMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {   // If the line is 0, it's the header, so we must skip it
            return;
        }

        String[] fields = value.toString().split(";");
        String district = fields[1];

        context.write(new Text(district), NullWritable.get());
    }
}