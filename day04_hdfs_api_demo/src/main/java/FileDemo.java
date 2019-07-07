import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class FileDemo {
    /*
    * 下载时将小文件合并
    * */
    @Test
    public void mergeFile() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        FSDataOutputStream outputStream = fileSystem.create(new Path("/bigfile.txt"));
        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //获取本地文件系统获取文件列表,为一个集合
        FileStatus[] fileStatuses = local.listStatus(new Path("file:///E:\\input"));
        for(FileStatus fileStatus:fileStatuses){
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream,outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();

    }



    /*
    * 使用代码准备下载文件
    * */
    @Test
    public void getCofig() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        fileSystem.copyToLocalFile(new Path("/cofig/core-site.xml"),new Path("file:///E:/core-site.xml"));
        fileSystem.close();

    }


    /*
    * hdfs文件上传
    * */
    @Test
    public  void putData() throws URISyntaxException, IOException, InterruptedException {
        //获取hdfs数据源对象
        FileSystem root = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        //向hdfs写入对象
        root.copyFromLocalFile(new Path("file:///E:\\c.xml"),new Path("/hello/mydir/test"));
        //关闭流对象
        root.close();
    }


    /*
    * 下载文件
    * */
    @Test
    public void getFileToLocal() throws URISyntaxException, IOException, InterruptedException {
        //获取hdfs的数据对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        //使用hdfs对象获取输出流对象
        FSDataInputStream inputStream = fileSystem.open(new Path("/b.xml"));
        //使用hdfs对象获取输入流对象
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\c.xml"));
        //关流
        IOUtils.copy(inputStream,outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }



    /*
    * HDFS上创建文件夹
    * */
    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"),new Configuration(),"root");
        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/mkdir/test"));
        fileSystem.close();
    }
    /*
    *  遍历 HDFS 中所有文件
    *使用 API 遍历
    *
    * */
    @Test
    public void ListMyFiles() throws IOException, URISyntaxException, InterruptedException {
        //获取fileSystem类
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        //获取RemoteIterator 得到所有文件或者文件夹,第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath().toString());
        }
        fileSystem.close();
    }



    /*
     * 使用文件系统方式访问数据
     * 第四种方式
     * */
    @Test
    public void getFilrSystem4() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println(fileSystem.toString());

    }

    /*
     * 使用文件系统方式访问数据
     * 第三种方式
     * */

    @Test
    public void getFilrSystem3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }


    /*
     * 使用文件系统方式访问数据
     * 第二种方式
     * */

    @Test
    public void getFilrSystem2() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println("fileSystem"+fileSystem);
    }

    /*
    * 使用文件系统方式访问数据
    * 第一种方式
    * */
    @Test
    public void getFilrSystem1() throws IOException {
        //1获取FileSystem对象
        Configuration configuration = new Configuration();
        //2指定我们使用的文件系统类型:
        configuration.set("fs.defaultFS","hdfs://node01:8020/");
        //3获取指定文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());

    }




    /*
    * 使用url方式访问数据
    *
    * */
    @Test
    public void DownLoad() throws IOException {
        //1获取hdfs对象的Url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //2获取文件输入流
        InputStream inputStream = new URL("hdfs://node01:8020/a.xml").openStream();
        //3获取文件输入流
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\hello.xml" ));
        //4实现文件拷贝
        IOUtils.copy(inputStream,outputStream);
        //5关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }
}
