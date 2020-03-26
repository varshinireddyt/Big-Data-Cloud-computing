import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable <Color>{
	public int type;       /* red=1, green=2, blue=3 */
	public int intensity;  /* between 0 and 255 */
	/* need class constructors, toString, write, readFields, and compareTo methods */

	public Color() { }

	public Color(int t, int i) {
		this.type = t;
		this.intensity = i;

	}

	public void setType(int type) {
		this.type = type;
	}

	public void setIntensity(int intensity) {
		this.intensity = intensity;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		out.writeInt(intensity);
	}

	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		intensity = in.readInt();
	}

	public String toString() {
		return this.type + "," + this.intensity;
	}

	public int compareTo(Color c) {
		return (this.type == c.type) ? this.intensity - c.intensity : this.type - c.type;
	}
}


public class Histogram {

	public static class HistogramMapper extends Mapper<Object,Text,Color, IntWritable> {
		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
			int red = scanner.nextInt();
			int green = scanner.nextInt();
			int blue = scanner.nextInt();

			Color redKey = new Color(1, red);
			Color greenKey = new Color(2, green);
			Color blueKey = new Color(3, blue);
			IntWritable opValue = new IntWritable(1);

			context.write(redKey, opValue);
			context.write(greenKey, opValue);
			context.write(blueKey, opValue);
			scanner.close();
		}
	}

	public static class HistogramInMapperCombiner extends Mapper<Object,Text,Color, IntWritable> {
//
//		 HashMap<Color, IntWritable> hTable;
//		
//		@Override
//		public void setup( Context context ) throws IOException, InterruptedException {
//			hTable = new HashMap<Color, IntWritable>();
//		}
//
//		@Override
//		public void cleanup(Context context ) throws IOException, InterruptedException {
//			Set<Color> keys = hTable.keySet();
//			for (Color key: keys) {
//				context.write(key, hTable.get(key));
//			}
//		}

		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
			int red = scanner.nextInt();
			int green = scanner.nextInt();
			int blue = scanner.nextInt();

			Color redKey = new Color(1, red);
			Color greenKey = new Color(2, green);
			Color blueKey = new Color(3, blue);
			IntWritable opValue = new IntWritable(1);

			context.write(redKey, opValue);
			context.write(greenKey, opValue);
			context.write(blueKey, opValue);
			scanner.close();
		}
	}

	public static class HistogramCombiner extends Reducer<Color,IntWritable,Color, IntWritable> {
		@Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context )
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable v: values) {
				sum += v.get();
			};

			context.write(key, new IntWritable(sum));
		}
	}

	public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
		@Override
		public void reduce ( Color key, Iterable<IntWritable> values, Context context )
				throws IOException, InterruptedException {
			/* write your reducer code */

			long sum = 0;
			for (IntWritable v: values) {
				sum += v.get();
			};
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main ( String[] args ) throws Exception {
		/* write your main program code */
		Job job = Job.getInstance();
		job.setJobName("MyJob");
		job.setJarByClass(Histogram.class);
		job.setJarByClass(Color.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapOutputKeyClass(Color.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(HistogramMapper.class);
		job.setCombinerClass(HistogramCombiner.class);
		job.setReducerClass(HistogramReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance();
		job2.setJobName("MyJob2");
		job2.setJarByClass(Histogram.class);
		job2.setJarByClass(Color.class);
		job2.setOutputKeyClass(Color.class);
		job2.setOutputValueClass(LongWritable.class);
		job2.setMapOutputKeyClass(Color.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setMapperClass(HistogramInMapperCombiner.class);
		job2.setReducerClass(HistogramReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2,new Path(args[0]));
		FileOutputFormat.setOutputPath(job2,new Path(args[1]+"2"));
		job2.waitForCompletion(true);
		
	}
}
