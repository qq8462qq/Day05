package partition;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //1创建Job任务对象
        Job job = Job.getInstance(super.getConf(), "partition_maperduce");
        job.setJarByClass(JobMain.class);
        //2对Job任务进行配置(八个步骤)
            //第一步:设置输入类和路径输入的
        job.setInputFormatClass(TextInputFormat.class);
       TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/input"));
       //TextInputFormat.addInputPath(job,new Path("file:///E:\\mapreduceB\\input"));

            //第二步:设置Mapper类和数据类型
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
            //第三步,指定分区类
        job.setPartitionerClass(MyPartitioner.class);
            //第四,五,六步(省略)
            //第七步:指定Reducer类和数据类型
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
            //设置ReduceTask的个数
        job.setNumReduceTasks(2);
            //第八步:指定输出类和输出路径\
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/out/partition_out"));
        //TextOutputFormat.setOutputPath(job,new Path("file:///E:\\mapreduceB\\output") );
       //3等待任务结束输出类型
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动JOB任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
