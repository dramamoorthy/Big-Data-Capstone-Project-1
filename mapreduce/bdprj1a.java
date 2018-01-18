//1 a) Is the number of petitions with Data Engineer job title increasing over time?

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;




public class bdprj1a {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split("\t");
	            long slno = Long.parseLong(str[7]);
	            if(str[4].equals("DATA ENGINEER"))
	            {
	            context.write(new Text(str[4]),new LongWritable(slno));
	         }}
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	public static class CaderPartitioner extends Partitioner < Text, LongWritable >
	   {
	      @Override
	      public int getPartition(Text key, LongWritable value, int numReduceTasks)
	      {
	         String[] str6 = value.toString().split("\t");
	         

	         if(str6[0].contains("2011"))
	         {
	            return 0 % numReduceTasks;
	         }
	         else if(str6[0].contains("2012"))
	         {
	            return 1 % numReduceTasks ;
	         }
	         else if(str6[0].contains("2013"))
	         {
	            return 2 % numReduceTasks;
	         }
	         else if(str6[0].contains("2014"))
	         {
	            return 3 % numReduceTasks;
	         }
	         else if(str6[0].contains("2015"))
	         {
	            return 4 % numReduceTasks;
	         }
	         else 
	         {
	            return 5 % numReduceTasks;
	         }
	         
	      }
	   }
	
	  public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
	   {
		    private LongWritable result = new LongWritable();
		    
		    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
		      
		      long count = 0;
		      
		         for ( LongWritable val : values)
		        	
		         {   
		        	 count++;     
		         }
		      
		      
		        
		      result.set(count);
		      
		      context.write(key, result);
		      
		    }
	   }
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Volume Count");
		    job.setJarByClass(StockVolume.class);
		    job.setMapperClass(MapClass.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setNumReduceTasks(6);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
