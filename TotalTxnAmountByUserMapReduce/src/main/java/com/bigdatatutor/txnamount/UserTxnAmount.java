package com.bigdatatutor.txnamount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserTxnAmount {

    public static class UserTxnAmountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] txnFieldsvalueArray = value.toString().split(",");
            Text userID = new Text(txnFieldsvalueArray[2]);
            IntWritable txnAmount = new IntWritable(Integer.parseInt(txnFieldsvalueArray[3]));
            context.write(userID, txnAmount);
        }
    }

    public static class UserTxnAmountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int totalTxnAmount = 0;
            for (IntWritable txnAmount : values) {
                totalTxnAmount = totalTxnAmount + txnAmount.get();
            }
            IntWritable totalTxnAmountWritable = new IntWritable(totalTxnAmount);
            context.write(key, totalTxnAmountWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total txn Amount");
        job.setJarByClass(UserTxnAmount.class);
        job.setMapperClass(UserTxnAmountMapper.class);
        job.setCombinerClass(UserTxnAmountReducer.class);
        job.setReducerClass(UserTxnAmountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}