import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.mapred.FileSplit;
import javax.naming.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

public class InvertedIndex {
	
	public static class SumMapper
       extends Mapper<LongWritable , Text, Text, Text>{

    private Text word = new Text();
    String documentId;
    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
    	documentId = itr.nextToken(); 
    	StringTokenizer ITR = new StringTokenizer(itr.nextToken());
    	
        while (ITR.hasMoreTokens()) {
        	word.set(ITR.nextToken());
            context.write(word,new Text(documentId));
        	}
    	}
  	}

	public static class SumReducer
       extends Reducer<Text,Text,Text,Text> {
    
	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
 
    	int sum=0;
    	String result = "";
    	    	
    	HashMap<String,Integer> map = new HashMap<String,Integer>();
    	
    	 for(Text val:values){
    		 if(map.containsKey(val.toString())){
    			 int x = map.get(val.toString());
    			 x++;
    			 map.put(val.toString(), x);
    		 }else{
    			 map.put(val.toString(), 1);
    		 }
    	 }
    	 for(String val : map.keySet()){
    		 result += val+":"+map.get(val)+" ";
    	 }
    	 context.write(key,new Text(result));
	}
}
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(SumMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
  }
}