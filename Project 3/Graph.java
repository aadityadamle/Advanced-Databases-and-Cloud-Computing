import java.io.*;
import java.util.Scanner;
import java.util.Vector;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                // true for a graph vertex, false for distance
    public int distance;               // the distance from the starting vertex
    public Vector<Integer> following;  // the vertex neighbors

    Tagged () { tag = false; }
    Tagged ( int d ) { tag = false; distance = d; }
    Tagged ( int d, Vector<Integer> f ) { tag = true; distance = d; following = f; }

    public void write ( DataOutput out ) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (tag) {
            out.writeInt(following.size());
            for ( int i = 0; i < following.size(); i++ )
                out.writeInt(following.get(i));
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for ( int i = 0; i < n; i++ )
                following.add(in.readInt());
        }
    }
}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    /*
    MAPPER 1
    */
    public static class ARD5625Mapper1 extends Mapper<Object, Text, IntWritable, IntWritable>{
	    @Override
	    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {

        //Read data
	    Scanner read = new Scanner(value.toString()).useDelimiter(",");
        int id = read.nextInt();
        int follower = read.nextInt();

        //Emit
        context.write(new IntWritable(follower), new IntWritable(id));
        read.close();
    }
}

/*
REDUCER 1
*/
   public static class ARD5625Reducer1 extends Reducer<IntWritable, IntWritable, IntWritable, Tagged>{
	   @Override
	   public void reduce(IntWritable follower, Iterable<IntWritable> ids, Context context) throws IOException, InterruptedException {
		Vector <Integer> following = new Vector<Integer>();   
        for (IntWritable id: ids){
			   following.addElement(id.get());
		   };

	   // starting vertex is either start_id (distributed mode) or 1 (local mode)
	  	   if(follower.get() == start_id ||follower.get() == 1){
			   //starting vertex has distance 0
			   context.write( follower, new Tagged(0,  following));
		   }else{ 
			   // all others have infinite distance (max_int is max integer)
			   context.write( follower, new Tagged(max_int, following)); // Output DataTypes : IntWritable and Tagged
	   }
    }

}

    /*
    MAPPER 2
    */
   // Mapper Class Parameters - (DataType of Input Key, DataType of Input Value, DataType of Output Key, DataType of Output Value)
   public static class ARD5625Mapper2 extends Mapper<IntWritable, Tagged, IntWritable, Tagged>{
        @Override   
        public void map(IntWritable follower, Tagged tagged, Context context) 
        throws IOException, InterruptedException{
	       context.write(follower, tagged); // Ouput Data Types are IntWritable and Tagged

           if(tagged.distance < max_int){
               //if(tagged.following != null){
               for (Integer id: tagged.following )
               {// propagate the follower distance + 1 to its following 
                    context.write(new IntWritable(id), new Tagged(tagged.distance + 1)); // Output Data Types are IntWrtable and Tagged changed to int so make new instance of tag and define its value to its new distance variable.
                };
            }
        }
    }

   /*
   REDUCER 2
   */
    public static class ARD5625Reducer2 extends Reducer<IntWritable, Tagged, IntWritable, Tagged>{
        @Override
        public void reduce(IntWritable id, Iterable<Tagged> values, Context context) throws IOException, InterruptedException {

            // this will have the minimum of all incoming distances and its own distance
            int max_v = Integer.MAX_VALUE;

            Vector<Integer> following = new Vector<Integer>();
	   // following = null;

            for (Tagged v: values){
                if(v.distance < max_v){
                    max_v = v.distance;
                }
                if(v.tag){
                    following = v.following;
                }
            };

            context.write(id, new Tagged(max_v, following));
        }
    }

    /*
    MAPPER 3
    */
    public static class ARD5625Mapper3 extends Mapper<IntWritable, Tagged, IntWritable, IntWritable >{
        @Override
        public void map(IntWritable follower, Tagged value, Context context) 
         throws IOException, InterruptedException{

            // keep only the vertices that can be reached
            if(value.distance < max_int){
                context.write(follower, new IntWritable (value.distance));
            }
        }
    }





    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        Job ARD5625J1 = Job.getInstance();
        /* ... First Map-Reduce job to read the graph */
        // Job 1
        ARD5625J1.setJobName("ARD5625J1");
        ARD5625J1.setJarByClass(Graph.class);
        ARD5625J1.setOutputKeyClass(IntWritable.class);
        ARD5625J1.setOutputValueClass(Tagged.class);
        ARD5625J1.setMapOutputKeyClass(IntWritable.class);
        ARD5625J1.setMapOutputValueClass(IntWritable.class);
        ARD5625J1.setMapperClass(ARD5625Mapper1.class);
        ARD5625J1.setReducerClass(ARD5625Reducer1.class);
        ARD5625J1.setInputFormatClass(TextInputFormat.class);
	    ARD5625J1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(ARD5625J1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(ARD5625J1, new Path(args[1]+"0"));
        ARD5625J1.waitForCompletion(true);
        /* ... */
        //FileInputFormat.setInputPaths(job,new Path(args[0]));
        //FileOutputFormat.setOutputPath(job,new Path(args[1]+"0"));
        //job.waitForCompletion(true);
        for ( short i = 0; i < iterations; i++ ) {
            //Job job = Job.getInstance();
            /* ... Second Map-Reduce job to calculate shortest distance */
            // Job 2
        Job ARD5625J2 = Job.getInstance();
        ARD5625J2.setJobName("ARD5625J2");
        ARD5625J2.setJarByClass(Graph.class);
        ARD5625J2.setOutputKeyClass(IntWritable.class);
        ARD5625J2.setOutputValueClass(Tagged.class);
        ARD5625J2.setMapOutputKeyClass(IntWritable.class);
        ARD5625J2.setMapOutputValueClass(Tagged.class);
        ARD5625J2.setMapperClass(ARD5625Mapper2.class);
        ARD5625J2.setReducerClass(ARD5625Reducer2.class);
        ARD5625J2.setInputFormatClass(SequenceFileInputFormat.class);
	    ARD5625J2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(ARD5625J2, new Path(args[1]+i));
	    FileOutputFormat.setOutputPath(ARD5625J2, new Path(args[1]+(i+1)));
        ARD5625J2.waitForCompletion(true);
            /* ... */
            //FileInputFormat.setInputPaths(job,new Path(args[1]+i));
            //FileOutputFormat.setOutputPath(job,new Path(args[1]+(i+1)));
            //job.waitForCompletion(true);
        }
        //job = Job.getInstance();
        /* ... Last Map-Reduce job to output the distances */
        Job ARD5625J3 = Job.getInstance();
        ARD5625J3.setJobName("ARD5625J3");
        ARD5625J3.setJarByClass(Graph.class);
        ARD5625J3.setOutputKeyClass(IntWritable.class);
        ARD5625J3.setOutputValueClass(IntWritable.class);
        ARD5625J3.setMapOutputKeyClass(IntWritable.class);
        ARD5625J3.setMapOutputValueClass(IntWritable.class);
        ARD5625J3.setMapperClass(ARD5625Mapper3.class);
        ARD5625J3.setInputFormatClass(SequenceFileInputFormat.class);
	    ARD5625J3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(ARD5625J3, new Path(args[1]+iterations));
	    FileOutputFormat.setOutputPath(ARD5625J3, new Path(args[2]));
        ARD5625J3.waitForCompletion(true);
        /* ... */
        //FileInputFormat.setInputPaths(job,new Path(args[1]+iterations));
        //FileOutputFormat.setOutputPath(job,new Path(args[2]));
        //job.waitForCompletion(true);
    }
}
