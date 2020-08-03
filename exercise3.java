import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise3 extends Configured implements Tool {

	// Mapper Input Key: Byte Offset of Line (LongWritable)
    // Mapper Input Value: line of file (Text)
    // Mapper Output Key: Concatenated string which contains Song Title, Artist's Name, Duration and Year (Text) 
    // Mapper Output Value: "" (Text)
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    
	// This query just needs a mapper which will filter based on the year
    private Text output_key = new Text();
    private Text output_val = new Text();
	
	public void configure(JobConf job) {
	}
//mapper function    	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> context, Reporter reporter) throws IOException {
        
		String line=value.toString();
		String[] tokens = line.split(",");

		int flag = 1;

		// Checks if the year field is integer
		try
            {
                double year_val = Double.parseDouble(tokens[165].trim());
            }
        catch (NumberFormatException e)
            {
                flag = 0;
            }

		if ((flag == 1 && Double.parseDouble(tokens[165]) >= 2000) && (Double.parseDouble(tokens[165]) <= 2010)){
			output_key.set(tokens[0]+","+tokens[2]+","+tokens[3]);
			output_val.set("");
			context.collect(output_key, output_val);
		}
    		
        }//mapper
    }//class 
   
   public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise3.class);
	conf.setJobName("exercise3");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(Text.class);
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(Map.class);	
    conf.setNumReduceTasks(0);
   
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
		

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise3(), args);
	System.exit(res);
    }
}
