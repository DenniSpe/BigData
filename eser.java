package esercitazione;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class eser {
	
	//Per essere più precisi, sarebbe meglio modificare i valori restituiti dal reducer
	//anzichè trattarli come Text
	
	
	static class myComparator implements WritableComparable<myComparator> {
		
		String k="";
		String v=""; 
		
		public myComparator() {}
		
		public myComparator(String s, String d) {
			this.k = s;
			this.v = d;
		}
		
		@Override
		public String toString() {
			return "MyComparable [k=" + k + ", v=" + v + "]";
		}
		
		
		@Override
		public void readFields(DataInput arg0) throws IOException {
			k = WritableUtils.readString(arg0);
			v = WritableUtils.readString(arg0);
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			WritableUtils.writeString(arg0, k);
			WritableUtils.writeString(arg0, v);
			
		}

		@Override
		public int compareTo(myComparator o) {
			
			Double a = Double.parseDouble(v);
			Double b = Double.parseDouble(o.v);
			if (k.compareTo(o.k)==0)
		     {
		       return (a.compareTo(b));
		     }
		     else return (k.compareTo(o.k));
		   }
			
			
			
	}
	

public static class compa extends WritableComparator {


  protected compa() {
	  super(myComparator.class, true);
  }
  
 
  public int compare(WritableComparable w1, WritableComparable w2) {
	  myComparator k1 = (myComparator)w1;
	  myComparator k2 = (myComparator)w2;
   
   if(k1.k.compareTo(k2.k) == 0)
   {
	 return Double.valueOf(k1.v).compareTo(Double.valueOf(k2.v));
   }
   return k1.k.compareTo(k2.k);
	  
	  
	  
  }
 }


public static class myGroup extends WritableComparator {

 protected myGroup() {
   super(myComparator.class, true);
  }
  
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
   myComparator k1 = (myComparator)w1;
   myComparator k2 = (myComparator)w2;
   
   return k1.k.compareTo(k2.k);
  }
 }
	
	
	
	
	public static class mySortedMap extends Mapper<Object, Text, myComparator, Text>
	{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, myComparator, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] part = value.toString().split("\t");
			
			if(part[14].contains("solved")) 
			{
				context.write(new myComparator(part[0], part[11]), new Text(part[11]));
			}
		}
	}

	
	public static class mySortedReducer extends Reducer<myComparator, Text, Text, Text>
	{

		@Override
		protected void reduce(myComparator key, Iterable<Text> values,
				Reducer<myComparator, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			String tmp="";
			for(Text t: values)
			{
				tmp+=t.toString()+" ";
			}
			
			tmp+=key.v;
			
			context.write(new Text(key.k), new Text(tmp));
		}
		
	}
	
	
	
	
	
	public static class myMap extends Mapper<Object, Text, Text, Text>{
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String[] part = value.toString().split("\t");
			
			
				context.write(new Text(part[0]), new Text(part[1]));
		}
	}
	
	
	
	public static class myRed extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {


			String tmp="";
			for(Text t: values)
			{
				tmp+=t.toString()+" ";
			}
	
			context.write(key, new Text(tmp));

		}
	}

	
	
	
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		
Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(eser.class);
		
		job.setMapperClass(mySortedMap.class);
		job.setReducerClass(mySortedReducer.class);
		job.setSortComparatorClass(compa.class);
		job.setGroupingComparatorClass(myGroup.class);
		
		job.setMapOutputKeyClass(myComparator.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		if(job.waitForCompletion(true)) {
			Configuration conf2 = new Configuration();
			
			Job job1 = Job.getInstance(conf2);
			
			job1.setJarByClass(eser.class);
			
			job1.setMapperClass(myMap.class);
			job1.setReducerClass(myRed.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			
			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/fine"));
			
			System.exit((job1.waitForCompletion(true)) ? 1 : 0);
			
		}
		
		
		
		System.exit((job.waitForCompletion(true)) ? 1 : 0);
		
	}

}
