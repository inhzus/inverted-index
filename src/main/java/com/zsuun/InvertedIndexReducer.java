package com.zsuun;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class InvertedIndexReducer extends
        Reducer<Text, IntWritable, Text, Text> {
    private String last = " ";
    private int fileCount;
    private int wordCount;
    private StringBuilder builder = new StringBuilder();

    private void write(Context context)
            throws IOException, InterruptedException {
        builder.setLength(builder.length() - 1);
        float frequency = (float) wordCount / fileCount;
        context.write(new Text(last),
                new Text(String.format("%.2f, %s",
                        frequency, builder.toString())));
    }

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {
        String[] splitKey = key.toString().split("#");
        String term = splitKey[0];
        if (!term.equals(last)) {
            if (!last.equals(" ")) {
                write(context);
                fileCount = 0;
                wordCount = 0;
                builder.setLength(0);
            }
            last = term;
        }
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        builder.append(String.format("%s: %d; ",
                splitKey[1], sum));
        fileCount += 1;
        wordCount += sum;
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        write(context);
    }
}
