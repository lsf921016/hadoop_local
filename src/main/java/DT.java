/**
 * Created by lenovo on 2017/12/10.
 */

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DT {
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            int index = 0;
            ArrayList<String> list = new ArrayList<String>();
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().toString();
                list.add(str);
            }
            String category = list.get(list.size() - 1);
            System.out.println("this map: " + this.hashCode());
            System.out.println("labels: " + category);
            for (int i = 0; i < list.size() - 1; ++i) {
                String attr = list.get(i) + ":" + category;
                context.write(new IntWritable(i), new Text(attr));
            }
        }
    }

    public static class AttributeReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void reduce(IntWritable attributeIndex, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            System.out.println("reducer begins");
            float splitPoint = 0;
            ArrayList<Float> set = new ArrayList<Float>();
            ArrayList<Float> vals = new ArrayList<Float>();
            ArrayList<Integer> labels = new ArrayList<Integer>();
            for (Text val : values) {
                String[] slices = val.toString().split(":");
                float v = Float.parseFloat(slices[0]);
                int label = Integer.parseInt(slices[1]);
                vals.add(v);
                labels.add(label);
                if (!set.contains(v)) {
                    set.add(v);
                }

            }
            Collections.sort(set);
            HashSet<Float> thresholds = new HashSet<Float>();
            for (int i = 0; i < set.size() - 1; ++i) {
                thresholds.add((set.get(i) + set.get(i + 1)) / 2);
            }
            double gini = Double.MAX_VALUE;
            int rows = vals.size();
            for (Float threshold : thresholds) {
                double lsize = 0;
                double rsize = 0;
                double lp = 0;
                double rp = 0;
                for (int i = 0; i < rows; ++i) {
                    if (vals.get(i) < threshold) {
                        lsize++;
                        if (labels.get(i) == 1 || labels.get(i) == 2 || labels.get(i) == 3) {
                            lp++;
                        }
                    } else {
                        rsize++;
                        if (labels.get(i) == 1 || labels.get(i) == 2 || labels.get(i) == 3) {
                            rp++;
                        }
                    }
                }

                double newGini = (lsize / rows) * (lp / lsize) * (1 - lp / lsize) + (rsize / rows) * (rp / rsize) * (1 - rp / rsize);
                if (newGini < gini) {
                    gini = newGini;
                    splitPoint = threshold;
                }
            }
            Text out = new Text(String.valueOf(gini) + "\t" + String.valueOf(splitPoint));
            System.out.println("this reducer: " + this.hashCode());
            System.out.println(attributeIndex.get() + " : " + out.toString());
            context.write(attributeIndex, out);
        }
    }

    public static void main(String[] args) throws Exception {
        //args0 : processedData
        //args1 : labels output_gini_path
        //args2 : output_predict_path
        //args3 : testx_path
        //args4 : testy_path
        long startTime = System.currentTimeMillis();
        String processedData=args[0];
        System.out.println(processedData);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(DT.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(AttributeReducer.class);
        job.setReducerClass(AttributeReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path in=new Path(args[0]);

        System.out.println("input path: "+args[0]);
        System.out.println("input path by Path"+in.toString());

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("exitCode: " + exitCode);
        Float score = predict(args[3], args[4], args[1], args[2]);
        System.out.println(score);
        long endTime = System.currentTimeMillis();
        System.out.println("This execution takes :" + (endTime - startTime) + "ms");
    }

    private static Float predict(String testxPath, String testyPath, String giniPath, String preditions_outputPath) throws IOException {
        //read gini
        //File f = new File(giniPath, "part-r-00000");
//        String baseUrl="hdfs://namenode:9000/user/ec2-user/";
//        testxPath=new Path(baseUrl,testxPath).toString();
//        testyPath=new Path(baseUrl,testyPath).toString();
//        giniPath=new Path(baseUrl,giniPath).toString();
//        preditions_outputPath=new Path(baseUrl,preditions_outputPath).toString();

        Path giniFile = new Path(giniPath,"part-r-00000");
        Configuration conf = new Configuration();
        FileSystem hdfs = giniFile.getFileSystem(conf);
        Scanner sc = new Scanner(hdfs.open(giniFile));
        PriorityQueue<GiniObj> heap = new PriorityQueue<GiniObj>(600, new GiniComparator() {
        });
        while (sc.hasNextLine()) {
            String[] slices = sc.nextLine().split("\t");
            int attributeIndex = Integer.parseInt(slices[0]);
            double gini = Double.parseDouble(slices[1]);
            double splitPoint = Double.parseDouble(slices[2]);
            heap.add(new GiniObj(attributeIndex, gini, splitPoint));
        }
        System.out.println(heap.size());
        GiniObj bestGini = heap.peek();
        sc.close();

        //read testx
        Path testxFile=new Path(testxPath);
        Scanner sc2 = new Scanner(hdfs.open(testxFile));
        ArrayList<Integer> preditions = new ArrayList<Integer>();
        ArrayList<Integer> labels = new ArrayList<Integer>();
        while (sc2.hasNextLine()) {
            String[] attributes = sc2.nextLine().split("\\s+");
            double value = Double.parseDouble(attributes[bestGini.getAttributeIndex() + 1]);
            if (value > bestGini.getSplitPoint()) {
                preditions.add(0);
            } else {
                preditions.add(1);
            }
        }
        //write preditions to file
        Path predictionFile=new Path(preditions_outputPath);
        PrintWriter pw = new PrintWriter(hdfs.create(predictionFile, true));
        for (int i = 0; i < preditions.size(); ++i) {
            pw.println(preditions.get(i));
        }
        pw.close();
        sc2.close();
        //read testy into array
        Path testyFile=new Path(testyPath);
        Scanner sc3 = new Scanner(hdfs.open(testyFile));
        while (sc3.hasNextLine()) {
            int label = Integer.parseInt(sc3.nextLine());
            labels.add(label);
        }
        return get_score(preditions, labels);
    }

    private static Float get_score(ArrayList<Integer> preditions, ArrayList<Integer> labels) {
        int size = preditions.size();
        float accurate = 0;
        for (int i = 0; i < size; ++i) {
            System.out.println("prediction: " + preditions.get(i) + "   " + "actual label: " + labels.get(i));
            if ((preditions.get(i) == 0 && (labels.get(i) == 1 || labels.get(i) == 2 || labels.get(i) == 3)) ||
                    (preditions.get(i) == 1 && (labels.get(i) == 4 || labels.get(i) == 5 || labels.get(i) == 6))) {
                accurate++;
            }
        }
        return accurate / size;
    }

    private static String mergeDataLabel(String dataPath, String labelPath) throws IOException {
        File dataFile = new File(dataPath);
        File labelFile = new File(labelPath);
        File processedData = new File(dataFile.getParent(), "processedData");
        if (processedData.exists()) {
            return processedData.getAbsolutePath();
        }
        BufferedReader brData = new BufferedReader(new FileReader(dataFile));
        BufferedReader brLabel = new BufferedReader(new FileReader(labelFile));
        PrintWriter pw = new PrintWriter(new FileWriter(processedData));
        String lineData = null;
        String lineLabel = null;
        while ((lineData = brData.readLine()) != null) {
            lineLabel = brLabel.readLine();
            pw.println(lineData + " " + lineLabel);
        }
        pw.close();
        return processedData.getAbsolutePath();
    }
}

class GiniComparator implements Comparator<GiniObj> {

    public int compare(GiniObj o1, GiniObj o2) {
        if (o1.gini - o2.gini > 0) {
            return 1;
        } else if (o1.gini - o2.gini < 0) {
            return -1;
        } else {
            return 0;
        }
    }
}

class GiniObj {
    int attributeIndex = 0;
    double gini = 0;
    double splitPoint = 0;

    GiniObj(int attributeIndex, double gini, double splitPoint) {
        this.attributeIndex = attributeIndex;
        this.gini = gini;
        this.splitPoint = splitPoint;
    }

    public int getAttributeIndex() {
        return attributeIndex;
    }

    public double getGini() {
        return gini;
    }

    public double getSplitPoint() {
        return splitPoint;
    }
}