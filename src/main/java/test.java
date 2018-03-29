import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by lenovo on 2017/10/13.
 */
public class test {
    public static void main(String[] args) {
        Path p=new Path("E:/hadoop_local/input");
        File[] files=new File(p.toString()).listFiles();
        ArrayList<String> list=new ArrayList<String>();
        for (File f: files){
            String str=f.getAbsolutePath();
            list.add(str.substring(str.lastIndexOf('\\'),str.length()));
        }
        System.out.println(list);
    }
}
