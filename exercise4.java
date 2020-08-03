import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.lang.Math; 

public class exercise4 extends Configured implements Tool {

    // Mapper Input Key: Byte Offset of Line (LongWritable)
    // Mapper Input Value: line of file (Text)
    // Mapper Output Key: Artist's Name (Text) 
    // Mapper Output Value: Duration (DoubleWritable)

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        public void configure(JobConf job) {
        }//configure
                
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException{
            
            String line=value.toString();
		    String[] tokens = line.split(",");   

            String artist = tokens[2];
            String duration = tokens[3];

            Double duration_val = Double.parseDouble(duration);

            output.collect(new Text(artist), new DoubleWritable(duration_val));
            
        }//mapper
    }//class

    public static class Partitioner_sort implements Partitioner<Text, DoubleWritable> {

        public void configure(JobConf job){
        }

        public int getPartition(Text key, DoubleWritable value, int Reducers){

            String line_2 = key.toString();
            line_2 = line_2.trim();
            char first_char = line_2.charAt(0);
            first_char = Character.toLowerCase(first_char);

            if(Reducers == 0)
            {
                return 0;
            }


            if (Character.valueOf(first_char)<=(Character.valueOf('f')))
            {
                return 0;
            }
            else if (Character.valueOf(first_char)>(Character.valueOf('f')) && Character.valueOf(first_char)<=(Character.valueOf('k')))
            {
                return 1;
            }
            else if (Character.valueOf(first_char)>(Character.valueOf('k')) && Character.valueOf(first_char)<=(Character.valueOf('p')))
            {
                return 2;
            }            
            else if (Character.valueOf(first_char)>(Character.valueOf('p')) && Character.valueOf(first_char)<=(Character.valueOf('u')))
            {
                return 3;
            }            
            else
            {
                return 4;
            }            
        }

        protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
        }
    }//Partitioner class



    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void configure(JobConf job) {
        }//configure
        protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
        }//setup

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            
            Double Max_Duration = 0.0;
            Double Curr_Duration = 0.0;

            while (values.hasNext()) 
            {
                Curr_Duration = values.next().get();                
                if (Curr_Duration > Max_Duration)
                {
                    Max_Duration = Curr_Duration;
                }
            }

            output.collect(key, new DoubleWritable(Max_Duration));
        }
        protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
        }//cleanup        
    }//Reduce Class

    //configurations
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), exercise4.class);
        conf.setJobName("exercise4");

        conf.setNumReduceTasks(5);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);        
        conf.setPartitionerClass(Partitioner_sort.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }//run

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new exercise4(), args);
        System.exit(res);
    }//main
}
