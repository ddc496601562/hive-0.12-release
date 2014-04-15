package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;


@SuppressWarnings("rawtypes")

public class GroupByAge extends Configured implements Tool {


	public static class Map extends
            Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

        int age;

        @Override
        protected void map(
                WritableComparable key,
                HCatRecord value,
                org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                        IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            age = (Integer) value.get(1);
            context.write(new IntWritable(age), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable,
    WritableComparable, HCatRecord> {


      @Override
      protected void reduce(
              IntWritable key,
              java.lang.Iterable<IntWritable> values,
              org.apache.hadoop.mapreduce.Reducer<IntWritable, IntWritable,
                      WritableComparable, HCatRecord>.Context context)
              throws IOException, InterruptedException {
          int sum = 0;
          Iterator<IntWritable> iter = values.iterator();
          while (iter.hasNext()) {
              sum++;
              iter.next();
          }
          HCatRecord record = new DefaultHCatRecord(2);
          record.set(0, key.get());
          record.set(1, sum);

          context.write(null, record);
        }
    }


	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String inputTableName = args[0];
        String outputTableName = args[1];
        String dbName = null;

        Job job = new Job(conf, "GroupByAge");
        HCatInputFormat.setInput(job, InputJobInfo.create(dbName,
                inputTableName, null));
        // initialize HCatOutputFormat

        job.setInputFormatClass(HCatInputFormat.class);
        job.setJarByClass(GroupByAge.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        //连接元数据库，初始化相应的OutputJobInfo ，然后Java序列化并存储到conf配置里面
        HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName,
                outputTableName, null));
        HCatSchema s = HCatOutputFormat.getTableSchema(job);
        System.err.println("INFO: output schema explicitly set for writing:"
                + s);
        //同样，序列化，然后设置到conf配置中  
        HCatOutputFormat.setSchema(job, s);
        job.setOutputFormatClass(HCatOutputFormat.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GroupByAge(), args);
        System.exit(exitCode);
    }
}