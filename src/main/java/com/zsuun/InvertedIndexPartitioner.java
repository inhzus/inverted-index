package com.zsuun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexPartitioner
        extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(
            Text key,
            IntWritable value,
            int numReduceTasks) {
        Text term = new Text(key.toString().split("#")[0]);
        return super.getPartition(term, value, numReduceTasks);
    }
}
