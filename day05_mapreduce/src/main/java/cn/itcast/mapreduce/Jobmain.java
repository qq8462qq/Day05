package cn.itcast.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class Jobmain extends Configured implements Tool {
    //该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //1创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "wordcont");
        job.setJarByClass(Jobmain.class);
        //2配置job任务对象(八个步骤)
        //第一步:指定文件的读取方式和路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcont"));
        /*TextInputFormat.addInputPath(job,new Path("file:///E:\\mapreduce\\input"));*/
        //第二步:指定map阶段的处理方式
        job.setMapperClass(WordCountMapper.class);
        //设置map阶段k2的类型
        job.setMapOutputKeyClass(Text.class);
        //设置map阶段v2的类型
        job.setMapOutputValueClass(LongWritable.class);
        //第三,四,五,六采用默认方式
        //第七步:指定reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReduce.class);
        //设置k3的类型
        job.setOutputKeyClass(Text.class);
        //设置v3的类型
        job.setOutputValueClass(LongWritable.class);
        //第八步:设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出路径
        Path path = new Path("hdfs://node01:8020/wordcount_out");
        TextOutputFormat.setOutputPath(job,path );
       /* TextOutputFormat.setOutputPath(job,new Path("file:///E:\\mapreduce\\output") );*/
        //获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        boolean b = fileSystem.exists(path);
        if (b){
            //删除目标目录
            fileSystem.delete(path,true);
        }
        //等待任务结束

        boolean bl = job.waitForCompletion(true);
        return bl?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动job任务

        int run = ToolRunner.run(configuration, new Jobmain(), args);
        System.exit(run);
    }
}
