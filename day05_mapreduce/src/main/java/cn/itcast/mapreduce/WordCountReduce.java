package cn.itcast.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
* 四个泛型解释
* KEYIN:k2类型
* VALULEIN:v2类型
*
* KEYOUT:k3类型
* VALUEOUT:v3类型
* */

public class WordCountReduce extends Reducer<Text, LongWritable,Text,LongWritable> {
    //reduce方法作用:将新的k2和v2转为k3和v3.将k3和v3写入上下文中
    /*
    *参数:
    *
    * key:新的k2
    * values :集合 上下文对象
    * context: 表示
    * -------------------------
    *如何将新的k2和v2转为k3和v3
    * 新 k2    v2
    * hello   <1,1,1>
    * world   <1,1>
    * hadoop  <1>
    *     -----------------
    *     k3     v3
    *  hello      3
    *  world      2
    *  hadoop     1
    * */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        //1遍历集合,将集合中的数字相加,得到v3
        for (LongWritable value : values){
            count +=value.get();
        }
        //2:将k3和v3写入上下文中
        context.write(key,new LongWritable(count) );
    }
}
