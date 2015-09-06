import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        //TODO
	Configuration conf = this.getConf();

	Job jobA = Job.getInstance(conf, "Orphan Pages");
	//jobA.setOutputKeyClass(IntWritable.class);
	//jobA.setOutputValueClass(NullWritable.class);
	//jobA.setOutputKeyClass(IntWritable.class);
	jobA.setMapOutputKeyClass(IntWritable.class);
	jobA.setMapOutputValueClass(IntWritable.class);
	
	jobA.setMapperClass(LinkCountMap.class);
	//jobA.setMapOutputKeyClass(IntWritable.class);
	//jobA.setMapOutputValueClass(IntWritable.class);
	jobA.setReducerClass(OrphanPageReduce.class);

	FileInputFormat.setInputPaths(jobA, new Path(args[0]));
	FileOutputFormat.setOutputPath(jobA, new Path(args[1]));
	
	jobA.setJarByClass(OrphanPages.class);
	return jobA.waitForCompletion(true)? 0 : 1;

	
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line, ": \t/");
	    int count = 0;
	    while(tokenizer.hasMoreTokens()) {
	        count ++;
		String nextToken = tokenizer.nextToken();
	 	if (count > 1) {
		    context.write(new IntWritable(Integer.parseInt(nextToken)), new IntWritable(1));
		} else {
		    context.write(new IntWritable(Integer.parseInt(nextToken)), new IntWritable(0));
		}
	    }

        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
            int sum = 0;
	    for (IntWritable val : values) {
		if (val.get() > 0) {
		   sum += val.get();
	           break;
		}
	    }
	    if (sum == 0){
		context.write(key, null);
	    }
        }
    }
}
