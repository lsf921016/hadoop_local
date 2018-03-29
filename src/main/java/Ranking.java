/**
 * Created by lenovo on 2017/10/13.
 */
/**
 * Created by lenovo on 2017/10/13.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
class WholeFileRecordReader2 extends RecordReader<Text,Text> {

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
class WholeFileInputFormat2 extends FileInputFormat<Text,Text> {

    public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        RecordReader<Text,Text> recordReader = new WholeFileRecordReader2();
        return recordReader;
    }

}
class Record{
    public int frequency;
    public String word;
    public Record(String word,int frequency){
        this.word=word;
        this.frequency=frequency;
    }
}
class RecordComparator implements Comparator<Record>{
    public int compare(Record a,Record b){
        return b.frequency - a.frequency;
    }
}

public class Ranking {

    public static class RankingMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text wordKey = new Text();
        private Text wordValue=new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lines=value.toString().split("\n");
            PriorityQueue<Record> heap=new PriorityQueue<Record>(4,new RecordComparator() );
            for (String line:lines){
                String[] strs=line.split("\t");
                Record record=new Record(strs[0],Integer.parseInt(strs[1]));
                heap.add(record);
            }

            StringBuilder ranking=new StringBuilder();
            while (!heap.isEmpty()){
                Record temp=heap.poll();
                ranking.append(temp.word);
                ranking.append(",");
            }
            ranking.delete(ranking.length()-1,ranking.length());
            InputSplit split=context.getInputSplit();
            String path=new String(split.toString());
            String[] path_slices=path.split("/");
            String stateName=path_slices[path_slices.length-2];
            ranking.replace(ranking.length()-1,ranking.length(),":");
            wordKey.set(ranking.toString());
            wordValue.set(stateName);
            context.write(wordKey,wordValue);

        }
    }

    public static class StatesSumReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text states = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for (Text t:values){
                sb.append(t.toString());
                sb.append(",");
            }
            sb.delete(sb.length()-1,sb.length());
            states.set(sb.toString());
            context.write(key, states);
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
        Job job = Job.getInstance(conf, "Ranking");
        job.setJarByClass(Ranking.class);
        job.setMapperClass(RankingMapper.class);
        job.setCombinerClass(StatesSumReducer.class);
        job.setReducerClass(StatesSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeFileInputFormat2.class);

        for (String state:states){
            WholeFileInputFormat2.addInputPath(job, new Path(args[0],state));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int exitCode=job.waitForCompletion(true) ? 0 : 1;
        System.out.println("----please check the path:  "+args[1].toString()+"   to see the output-------------");
        if (exitCode==1){
            System.exit(exitCode);
        }
        System.exit(0);
    }
}

