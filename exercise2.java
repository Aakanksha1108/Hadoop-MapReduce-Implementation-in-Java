import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise2 extends Configured implements Tool {

	// Mapper Input Key: Byte Offset of Line (IntWritable)
    // Mapper Input Value: line of file (Text)
    // Mapper Output Key: Word (Text)
    // Mapper Output Value: Number of Volumes + (Number of Volumes)^2 (Text)

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	     
            String line = value.toString();
	        String[] tokens = line.split("\\s+"); 

			String volume_str;

			// Extract volume from the line. When the line has 4 tokens, volume is at the 3rd position and when line has 5 tokens, volume is at the 4th position
	        if (tokens.length == 4)
			{
				volume_str = tokens[3].trim();
			}
			else
			{
				volume_str = tokens[4].trim();
			}

			// Calculate the below quantities to 
	        double volume = Double.parseDouble(volume_str);
	        double square_of_volume = Math.pow(volume,2);

	        //collect output
	        output.collect(new Text("Standard Deviation of all volume values = "), new Text(Double.toString(volume)+","+Double.toString(square_of_volume)));
	    }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            
			double sum = 0.0;
			double N = 0.0;
			double sum_of_squares = 0.0;

	        while (values.hasNext()) { 
			String[] tokens = values.next().toString().split(",");
			    N++;
		        sum = sum + Double.parseDouble(tokens[0]); 
		        sum_of_squares = sum_of_squares + Double.parseDouble(tokens[1]); 
		    }
			
		    double standard_deviation = Math.sqrt(((1/N)*(sum_of_squares)) - Math.pow(((1/N)*sum),2));
		    output.collect(key, new DoubleWritable(standard_deviation));
        }
    }

    public int run(String[] args) throws Exception {
	    JobConf conf = new JobConf(getConf(), exercise2.class);
	    conf.setJobName("exercise2");
   
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);
    
	    conf.setMapperClass(Map.class);
	    conf.setReducerClass(Reduce.class);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);  
    
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
	    JobClient.runJob(conf);
	    return 0;
    }

    public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new exercise2(), args);
	    System.exit(res);
    }
}