import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
	public short tag;                 // 0 for a graph vertex, 1 for a group number
	public long group;                // the group where this vertex belongs to
	public long VID;                  // the vertex ID
	public Vector<Long> adjacent;     // the vertex neighbors

	public Vertex() { }

	public Vertex(short tag, long group, long VID, Vector<Long> adjacent) {
		this.tag = tag;
		this.group = group;
		this.VID = VID;
		this.adjacent = adjacent;
	}

	public Vertex(short tag, long group) {
		this.tag = tag;
		this.group = group;
	}

	public void write(DataOutput out) throws IOException {
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(VID);
		out.writeInt((adjacent == null ? 0 : adjacent.size()));

		if (adjacent != null) {
			for (Long adjacentValue: this.adjacent) {
				out.writeLong(adjacentValue);
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		tag = in.readShort();
		group = in.readLong();
		VID = in.readLong();
		
		int size = in.readInt();
		adjacent = new Vector<Long>(size);

		for (int i = 0; i < size; i++) {
			long elem = in.readLong();
			adjacent.add(elem);
		}
	}
}


public class Graph {
	public static class VertexMapperOne extends Mapper<Object, Text, LongWritable, Vertex> {
		@Override
		public void map (Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
			long VID = scanner.nextLong();
			Vector<Long> adjacent = new Vector<Long>();
			
			while (scanner.hasNext()) {
				adjacent.addElement(scanner.nextLong());
			}

			Vertex vertex = new Vertex((short)0, VID, VID, adjacent);
			context.write(new LongWritable(VID), vertex);

			scanner.close();
		}
	}

	public static class VertexMapperTwo extends Mapper<Object, Vertex, LongWritable, Vertex> {
		@Override
		public void map (Object key, Vertex vertex, Context context)
				throws IOException, InterruptedException {

			context.write(new LongWritable(vertex.VID), vertex);

			for (Long adjacentVertex: vertex.adjacent) {
				context.write(new LongWritable(adjacentVertex), new Vertex((short) 1, vertex.group));
			}
		}
	}

	public static class VertexReducerTwo extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
	    public void reduce(LongWritable vid, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
	        long m = Long.MAX_VALUE;
	        Vector<Long> adjacent = null;
	        
	        for (Vertex v : values) {
	            if (v.tag == 0)
	            	adjacent = (Vector<Long>) v.adjacent.clone();
	            m = Math.min(m, v.group);
	        }

	        context.write(new LongWritable(m), new Vertex((short) 0, m, vid.get(), adjacent));
	    }
	}

	public static class VertexMapperFinal extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
		@Override
		public void map (LongWritable group, Vertex value, Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(group.get()), new LongWritable(1));
		}
	}

	public static class VertexReducerFinal extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		@Override
		public void reduce (LongWritable group, Iterable<LongWritable> values, Context context )
				throws IOException, InterruptedException {
			long counter = 0;
			
			for (LongWritable value: values) {
				counter += value.get();
			}
			
			context.write(new LongWritable(group.get()), new LongWritable(counter));
		}
	}


	public static void main ( String[] args ) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("MyJob");
		job1.setJarByClass(Graph.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Vertex.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Vertex.class);
		job1.setMapperClass(VertexMapperOne.class);

		job1.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/f0"));
		job1.setNumReduceTasks(0);
		job1.waitForCompletion(true);

		for ( short i = 0; i < 5; i++ ) {
			Job job2 = Job.getInstance();
			job2.setJobName("MyJob2");
			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			job2.setMapperClass(VertexMapperTwo.class);
			job2.setReducerClass(VertexReducerTwo.class);

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(job2, new Path(args[1]+"/f"+i));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/f"+(i+1)));
			job2.waitForCompletion(true);
		}

		Job job3 = Job.getInstance();
		job3.setJobName("MyJob3");
		job3.setJarByClass(Graph.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);
		job3.setMapperClass(VertexMapperFinal.class);
		job3.setReducerClass(VertexReducerFinal.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]+"/f5"));
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		job3.waitForCompletion(true);
	}
}
