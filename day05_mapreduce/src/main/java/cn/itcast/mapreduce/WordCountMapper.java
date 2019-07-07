package cn.itcast.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
/*
* 四个泛型解释
* KEYIN:k1的类型
* VALUEIN:v1的类型
*
* KEYOUT:k2的类型
* VALUEOUT:v2的类型
* */

public class WordCountMapper extends Mapper<LongWritable,Text, Text,LongWritable> {
   /*map方法就是将k1和v1转为k2和v2
   *参数:
   * key :k1  行的偏移量
   * value v1: 每一行的文本shuju
   * context: 表示上下文对象
   * */
   //如何将k1和v1转成k2和v2
    /*假如  k1        v1
    *      0         hello ,world , hadoop
    *      15        hadoop ,world ,hive
    *     ----------------------------------
    *
    *     k2         v2
    *     hello       <1>
    *     world      <1>
    *     hadoop      <1>
    *     hive      <1>
    *     hello      <1>
    * */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       Text text=new Text();
        LongWritable longWritable = new LongWritable();
        //1将一行的文本数据进行拆分
        String[] split = value.toString().split(",");
        // 2遍历数组,组装k2和v2String
        for (String word:split){
            //3将k2和v2写入上下文
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }


    }
}
