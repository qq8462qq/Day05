package partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
* k1:行偏移量LongWritable
* v1:行文本数据 Text
*k2 :行文本数据 Text
* v2:nullWritable
* */

public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //map方法把k1和v1转k2和v2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}
