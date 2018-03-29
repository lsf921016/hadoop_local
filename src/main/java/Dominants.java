/**
 * Created by lenovo on 2017/10/14.
 */



import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
class WholeFileRecordReader extends RecordReader<Text,Text> {

    private FileSplit fileSplit;
    private JobContext jobContext;
    private Text currentKey = new Text();
    private Text currentValue = new Text();
    private boolean finishConverting = false;
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        float progress = 0;
        if(finishConverting){
            progress = 1;
        }
        return progress;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) arg0;
        this.jobContext = arg1;
        String filename = fileSplit.getPath().getName();
        this.currentKey = new Text(filename);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        if(!finishConverting){
            int len = (int)fileSplit.getLength();
//          byte[] content = new byte[len];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
            FSDataInputStream in = fs.open(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(in,"gbk"));
//          BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
            String line="";
            String total="";
            while((line= br.readLine())!= null){
                total =total+line+"\n";
            }
            br.close();
            in.close();
            fs.close();
            currentValue = new Text(total);
            finishConverting = true;
            return true;
        }
        return false;
    }

}
class WholeFileInputFormat extends FileInputFormat<Text,Text> {

    public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        RecordReader<Text,Text> recordReader = new WholeFileRecordReader2();
        return recordReader;
    }

}
public class Dominants {

    public static class DominantsMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lines=value.toString().split("\n");
            HashMap<Integer,String> map=new HashMap<Integer, String>();
            for (String line:lines){
                String[] strs=line.split("\t");
                map.put(Integer.parseInt(strs[1]),strs[0]);
            }
            int keyMax=0;
            for (Map.Entry entry:map.entrySet()){
                int thisKey=Integer.parseInt(entry.getKey().toString());
                if (thisKey>keyMax){
                    keyMax=thisKey;
                }
            }
            word.set(map.get(new Integer(keyMax)));
            context.write(word,one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String[] states={"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware"
                , "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
                "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
                "New_Hampshire", "New_Jersey", "New_Mexico", "New_York", "North_Carolina", "North_Dakota", "Ohio", "Oklahoma",
                "Oregon", "Pennsylvania", "Rhode_Island", "South_Carolina", "South_Dakota", "Tennessee", "Texas", "Utah",
                "Vermont", "Virginia", "Washington", "West_Virginia", "Wisconsin", "Wyoming"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "who are dominants");
        job.setJarByClass(Dominants.class);
        job.setMapperClass(DominantsMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(WholeFileInputFormat2.class);

        for (String state:states){
            WholeFileInputFormat2.addInputPath(job, new Path(args[0],state));
        }
//        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int exitCode=job.waitForCompletion(true) ? 0 : 1;
        System.out.println("----please check the path:  "+args[1].toString()+"   to see the output----------");
        if (exitCode==1){
            System.exit(1);
        }
        System.exit(0);
    }
}


