import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO
        Configuration conf = this.getConf();
	FileSystem fs = FileSystem.get(conf);
	Path tmpPath = new Path("/mp2/tmp");
	fs.delete(tmpPath, true);
	
	Job jobA = Job.getInstance(conf, "Link Count");
	jobA.setOutputKeyClass(IntWritable.class);
	jobA.setOutputValueClass(IntWritable.class);
	
	jobA.setMapperClass(LinkCountMap.class);
	jobA.setReducerClass(LinkCountReduce.class);
	
	FileInputFormat.setInputPaths(jobA, new Path(args[0]));
	FileOutputFormat.setOutputPath(jobA, tmpPath);
	
	jobA.setJarByClass(PopularityLeague.class);
	jobA.waitForCompletion(true);
	
	Job jobB = Job.getInstance(conf, "League Links");
	jobB.setOutputKeyClass(IntWritable.class);
	jobB.setOutputValueClass(IntWritable.class);
	
	jobB.setMapOutputKeyClass(NullWritable.class);
	jobB.setMapOutputValueClass(IntArrayWritable.class);
	
	jobB.setMapperClass(LeagueLinksMap.class);
	jobB.setReducerClass(LeagueLinksReduce.class);
	jobB.setNumReduceTasks(1);	
	
	jobB.setInputFormatClass(KeyValueTextInputFormat.class);
	jobB.setOutputFormatClass(TextOutputFormat.class);
	
	jobB.setJarByClass(PopularityLeague.class);
	return jobB.waitForCompletion(true) ? 0 : 1;
    }

	
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
	Path pt= new Path(path);
	FileSystem fs = FileSystem.get(pt.toUri(), conf);
	FSDataInputStream file = fs.open(pt);
	
	BufferedReader buffIn= new BufferedReader(new InputStreamReader(file));
	
	StringBuilder everything = new StringBuilder();
	String line;
	while ((line = buffIn.readLine())!= null)  {
	    everything.append(line);
	    everything.append("\n");
	}
	return everything.toString();
    }


    public static class IntArrayWritable extends ArrayWritable {
    	public IntArrayWritable() {
	    super(IntWritable.class);
	}
	
	public IntArrayWritable(Integer[] numbers) {
	    super(IntWritable.class);
	    IntWritable[] ints = new IntWritable[numbers.length];
	    for (int i = 0; i < numbers.length; i++) {
		ints[i] = new IntWritable(numbers[i]);
            }
	    set(ints);
	}
    }
    
    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
	List<String> league;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    
	    String leaguePath = conf.get("league");
	    
	    this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line, ": \t/");
 	    int count = 0;
	    while (tokenizer.hasMoreTokens()) {	
		count ++;
		String nextToken = tokenizer.nextToken();
		if (count > 1){
		    if (league.contains(nextToken)){
		    	context.write(new IntWritable(Integer.parseInt(nextToken)), new IntWritable(1));
		    }
		}
	    }
	}
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	     int sum = 0;
	     for (IntWritable val : values){
	    	 sum += val.get();
 	     }
	     context.write(key, new IntWritable(sum));
	}
    }

    // TODO
    
     public static class LeagueLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
     	private TreeSet<Pair<Integer, Integer>> countToLinkMap = new TreeSet<Pair<Integer, Integer>> ();
	
	@Override
	public void map (Text key, Text value, Context context) throws IOException, InterruptedException {
	    Integer count = Integer.parseInt(value.toString());
	    Integer link = Integer.parseInt(key.toString());
	
	    countToLinkMap.add(new Pair<Integer, Integer>(count, link));

	    if (countToLinkMap.size() > 16) {
		countToLinkMap.remove(countToLinkMap.first());
	    }
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	    for (Pair<Integer, Integer> item : countToLinkMap) {
		Integer[] ints = {item.second, item.first};
		IntArrayWritable val = new IntArrayWritable(ints);
	        context.write(NullWritable.get(), val);
	    }
	}
     }

     public static class LeagueLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
	private TreeSet<Pair<Integer, Integer>> countToLinkMap = new TreeSet<Pair<Integer, Integer>>();
		
	@Override
	public void reduce (NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
	   // IntArrayWritable prevValue =  values[0];
	   // Text[] pair = (Text[]) prevValue.toArray();
	    Integer linkPrev= 0; //= Integer.parseInt(pair[0].toString());
	    Integer countPrev = 0;// = Integer.parseInt(pair[1].toString());	    

	    int prev = 0;
	    int interval = 1;
	    int countAll = 0;
	    for (IntArrayWritable val : values){
		Text[] pair = (Text[]) val.toArray();
		Integer link = Integer.parseInt(pair[0].toString());
	        Integer count = Integer.parseInt(pair[1].toString());	
	        if (countAll > 0 && count == countPrev) {
	 	    context.write(new IntWritable(link), new IntWritable(prev));
	            interval ++;
	        } else {
		    context.write(new IntWritable(link), new IntWritable(prev + interval));
		    prev += interval;
	            interval = 1;
	        }
		linkPrev = link;
		countPrev = count;
		countAll++;
	    } 
	}

    }


    class Pair <A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {
	public final A first;
	public final B second;
	
	public Pair(A first, B second) {
	   this.first = first;
	   this.second = second;
	}

	public static <A extends Comparable<? super A>,
		 B extends Comparable<? super B>> 
	Pair<A, B> of(A first, B second)
 	{
	    return new Pair<A, B> (first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
	    int cmp = o == null ? 1 : (this.first).compareTo(o.first);
	    return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override 
	public int hashCode() {
	    return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode (Object o) {
	    return o == null ? 0 : o.hashCode();
	}	 

	@Override
	public boolean equals(Object obj){
	   if (!(obj instanceof Pair))
	       return false;
	   if (this == obj)
		return true;
	   return equal(first, ((Pair<?, ?>) obj).first) && equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
	    return o1 == o2 || (o1 != null && o1.equals(o2));

	}
	
	@Override
	public String toString() {
	    return "(" + first + ", " + second + ")";
	}
    }
}
