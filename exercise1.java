import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class exercise1 extends Configured implements Tool {
		
	// Mapper Input Key: Byte Offset of Line (IntWritable)
    // Mapper Input Value: line of file (Text)
    // Mapper Output Key: Year and substring (Text) - Example "2000, nu, " 
    // Mapper Output Value: Number of volumes (IntWritable)

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    
	    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	     
            String line = value.toString();
	        String[] tokens = line.split("\\s+"); 

	        // For records from the 1gram file
	        if (tokens.length == 4)
			{
				int flag = 1;

				// Checks if the year field is integer
				try
                    {
                        int year_val = Integer.parseInt(tokens[1].trim());
                    }
                    catch (NumberFormatException e)
                    {
                        flag = 0;
                    }

				// Proceeds with the subsequent steps only if the year is of type integer
				if (flag == 1)
				{
					String word = tokens[0].trim();
					String year = tokens[1].trim();
					String number_of_volumes = tokens[3].trim();
					
					int volume = Integer.parseInt(number_of_volumes);
                    IntWritable volumes = new IntWritable(volume);

	        		if (word.toLowerCase().contains("nu"))
					{
	        			String key_val = year + "," + " nu, ";
	        			output.collect(new Text(key_val),volumes);
	        		}
	        		if (word.toLowerCase().contains("chi"))
					{
	        			String key_val = year + "," + " chi, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
	        		if (word.toLowerCase().contains("haw"))
					{
	        			String key_val = year + "," + " haw, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
				}
	        }

	         // For records from the 2gram file
	        if (tokens.length == 5)
			{
				int flag = 1;

				// Checks if the year field is integer
				try
                    {
                        int year_val = Integer.parseInt(tokens[2].trim());
                    }
                    catch (NumberFormatException e)
                    {
                        flag = 0;
                    }

	        	// Proceeds with the subsequent steps only if the year is of type integer
	        	if (flag == 1)
				{
					String first_word = tokens[0].trim();
					String second_word = tokens[1].trim();
					String year = tokens[2].trim();
					String number_of_volumes = tokens[4].trim();
					
					int volume = Integer.parseInt(number_of_volumes);
                    IntWritable volumes = new IntWritable(volume);
	        		
	        		// First word
	        		if (first_word.toLowerCase().contains("nu"))
					{
	        			String key_val = year + "," + " nu, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
	        		if (first_word.toLowerCase().contains("chi"))
					{
	        			String key_val = year + "," + " chi, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
	        		if (first_word.toLowerCase().contains("haw"))
					{
	        			String key_val = year + "," + " haw, ";
	        			output.collect(new Text(key_val), volumes);
	        		}

	        		// Second word
	        		if (second_word.toLowerCase().contains("nu"))
					{
	        			String key_val = year + "," + " nu, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
	        		if (second_word.toLowerCase().contains("chi"))
					{
	        			String key_val = year + "," + " chi, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
	        		if (second_word.toLowerCase().contains("haw"))
					{
	        			String key_val = year + "," + " haw, ";
	        			output.collect(new Text(key_val), volumes);
	        		}
	        	}

	        }
	    }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {

    //Reducer Input Key: Year and substring (Text)
    //Reducer Input Values: Volumes (IntWritable)
    //Reducer Output Key: Year and substring (Text)
    //Reducer Output Value: Average of Volumes (DoubleWritable)

	    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {           
			double sum = 0.0, avg = 0.0;
            int n = 0;

            while (values.hasNext()) 
            {
                sum += values.next().get();
                n += 1;
            }
			avg = sum/n;
            output.collect(key, new DoubleWritable(avg));
        }
    }

    public int run(String[] args) throws Exception {
	    JobConf conf = new JobConf(getConf(), exercise1.class);
	    conf.setJobName("exercise1");
   
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(IntWritable.class);
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
	    int res = ToolRunner.run(new Configuration(), new exercise1(), args);
	    System.exit(res);
    }
}
