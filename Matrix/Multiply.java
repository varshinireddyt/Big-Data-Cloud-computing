import java.io.*;
import java.util.ArrayList;
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
import org.apache.hadoop.util.ReflectionUtils;
class Elem implements Writable {
	int tag;
	int index;
	double value;

	public Elem() {}
	public Elem(Elem another) {
		this.tag = another.tag;
		this.value = another.value;
		this.index =  another.index;
	}
	public Elem(int index, int tag, double value) {
		this.tag = tag;
		this.index = index;
		this.value = value;
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		tag = input.readInt();
		index = input.readInt();
		value = input.readDouble();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(tag);
		output.writeInt(index);
		output.writeDouble(value);
	}

}

class Pair implements WritableComparable<Pair> {
	int i;
	int j;
	public Pair() {}
	public Pair(int i,int j) {
		this.i = i;
		this.j = j;
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		i = input.readInt();
		j = input.readInt();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(i);
		output.writeInt(j);
	}
	@Override
	public int compareTo(Pair p) {
		if( i< p.i) { return -1; }
		else if (i > p.i) {
			return 1;
		}
		else {
			if(j < p.j) { return -1; }
			else if(j > p.j) { return 1; }

		}
		return 0;
	}
}


public class Multiply {
	public static class MapperM extends Mapper<Object,Text, IntWritable, Elem> {
		public void map(Object key, Text value, Context context )throws IOException, InterruptedException {

		Scanner s = new Scanner(value.toString()).useDelimiter(",");
		Elem e = new Elem(s.nextInt(),s.nextInt(), s.nextDouble());
		Elem mulValue = new Elem(0,e.tag,e.value);
 		context.write(new IntWritable(e.index), mulValue );
		}
	}
	public static class MapperN extends Mapper<Object,Text, IntWritable, Elem> {
		public void map(Object key, Text value, Context context )throws IOException, InterruptedException {

		Scanner s = new Scanner(value.toString()).useDelimiter(",");
		Elem e = new Elem(s.nextInt(),s.nextInt(), s.nextDouble());
		Elem mulValue = new Elem(0,e.index,e.value);
 		context.write(new IntWritable(e.tag), mulValue );
		}
	}
	public static class HistogramReducer extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
		@Override
		public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
				throws IOException, InterruptedException {
			/* write your reducer code */
			ArrayList<Elem> listM = new ArrayList<Elem>();
			ArrayList<Elem> listN = new ArrayList<Elem>();
			for(Elem element: values) {
				Elem newE = new Elem(element);
				Elem newObj = ReflectionUtils.newInstance(Elem.class, conf);
				ReflectionUtils.copy(conf, element, newObj);
				if(newE.tag == 0) {
					listM.add(newE);
				}
				else if(newE.tag == 1) {
					listN.add(newE);
				}
			}

			for(int i=0; i<listM.size(); i++) {
				for(int j=0; j<listN.size(); j++) {
					Pair p = new Pair(listM.get(i).index,listN.get(j).index);
					double res = listM.get(i).value * listN.get(j).value;
					context.write(p, new DoubleWritable(res));
				}
			}
		 }
	  }
	public static class MapperAdd extends Mapper<Object,Text, IntWritable, Elem> {
		public void map(Object key, Text value, Context context )throws IOException, InterruptedException {

			String readLine = value.toString();
			String[] pairValue = readLine.split("");
			Pair p = new Pair(Integer.parseInt(pairValue[0]),Integer.parseInt(pairValue[1]));
			DoubleWritable val = new DoubleWritable(Double.parseDouble(pairValue[2]));
			context.write(p, val);
		}
	}
	public static class ReducerAdd extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
	@Override
	public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
			throws IOException, InterruptedException {
		/* write your reducer code */

		double sum = 0;
		for (DoubleWritable v: values) {
			sum += v.get();
		};
		context.write(key, new DoubleWritable(sum));
	}
	}

}
}
