//5) Find the most popular top 10 job positions for H1B visa applications for each year?
//a) for all the applications

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




  public class bdprj5atree {
		public static class Top5Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
			public void map(LongWritable key, Text value, Context context
	        ) throws IOException, InterruptedException {
				try {
				String[] str = value.toString().split("\t");
				String job_pos= str[4];
				String str2 = str[7];
				context.write(new Text(job_pos),new Text(str2));
		           
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
		
		public static class CaderPartitioner extends Partitioner < Text, Text >
		   {
		      @Override
		      public int getPartition(Text key, Text value, int numReduceTasks)
		      {
		         String[] str6 = value.toString().split(",");
		         

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
	

		public static class Top5Reducer extends
		Reducer<Text, Text, NullWritable, Text> {
			private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

			public void reduce(Text key, Iterable<Text> values,
					Context context) throws IOException, InterruptedException {
				long sum=0;
				String totcnt= "";
				String year="";
					for (Text val : values) {
						
						 String[] str3=val.toString().split(",");
			        	 LongWritable value = new LongWritable(Integer.parseInt(str3[0]));
			        	 year= value.toString();
			        	 sum=sum+1;
					}
					totcnt= key.toString();
					totcnt= totcnt + ',' + sum+','+year;
					repToRecordMap.put(new Long(sum), new Text(totcnt));
					if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
							}
			}
			
					protected void cleanup(Context context) throws IOException,
					InterruptedException {
					for (Text t : repToRecordMap.descendingMap().values()) {
						context.write(NullWritable.get(), t);
						}
					}
			
		}
			
		public static void main(String[] args) throws Exception {
			
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "5a");
		    job.setJarByClass(bdprj5atree.class);
		    job.setMapperClass(Top5Mapper.class);
		    job.setPartitionerClass(CaderPartitioner.class);
		    job.setReducerClass(Top5Reducer.class);
		    job.setNumReduceTasks(6);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}