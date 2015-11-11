package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import edu.asu.cse512.Tuple;
import edu.asu.cse512.TuplePair;

@SuppressWarnings("serial")
public class ClosestPair implements Serializable
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main(String[] args) {
    	if(args.length < 2){
    		System.out.println("Closest Pair expects atleast 2 arguments, inputLocation and outputLocation. Exiting..");
    		return;
    	}
    	deleteFilesIfExists(args[1]);
    	SparkConf conf = new SparkConf().setAppName("Closest Pair");
        JavaSparkContext context = new JavaSparkContext(conf);
        //Initialize, need to remove existing in output file location.
    	
    	//Implement 
    	
        try {
        	JavaRDD<String> inputRDD = context.textFile(args[0]);
        	closestPair(inputRDD, context, args[1]);
        } catch (Exception ex) {
        	System.out.println("Exception occured processing the input" + ex.getMessage());
        }
    }
    
    private static void deleteFilesIfExists(String outputPath) {
    	//Delete any output files if present
    	Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create(outputPath), conf);
	        hdfs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    private static void closestPair(JavaRDD<String> input, JavaSparkContext context, String outputPath) throws Exception {
		JavaRDD<Tuple> inputTuples = processInput(input);
		JavaRDD<Tuple> sortedInputTuples = sortTuples(inputTuples);
		List<Tuple> tuples = sortedInputTuples.collect();
		JavaRDD<TuplePair> closestPointCollection = collectClosestPoints(context, tuples);
		TuplePair closestPair = reduceClosestPointCollection(closestPointCollection);
		if (closestPair != null) {
			List<Tuple> closestPairList = closestPair.asList();
			Collections.sort(closestPairList, new Comparator<Tuple>() {
				public int compare(Tuple o1, Tuple o2) {
					int result = Double.compare(o1.getX(), o2.getX());
		               return (result == 0 ? Double.compare(o1.getY(), o2.getY()) : result);
		            }
		        });
			JavaRDD<Tuple> result = context.parallelize(closestPairList).repartition(1);
			result.saveAsTextFile(outputPath);
		} else {
			System.out.println("No closest pair found.");
		}
	}

	private static JavaRDD<Tuple> processInput(JavaRDD<String> inputRDD){
		JavaRDD<Tuple> pointsRDD = inputRDD.map(new Function<String, Tuple>(){
			public Tuple call(String line) throws Exception {
				String coordinates[] = line.split(",");
				double x = Double.valueOf(coordinates[0]);
				double y = Double.valueOf(coordinates[1]);
				Tuple tuple = new Tuple(x, y);
				return tuple;
			}
		});
		
		return pointsRDD;
	}
	
	private static JavaRDD<Tuple> sortTuples(JavaRDD<Tuple> inputTuples) {
		JavaRDD<Tuple> sortedInputTuples = inputTuples.sortBy(new Function<Tuple, Double>() {
			public Double call(Tuple tuple) throws Exception {
			    return tuple.getX();
			  }
			}, true, 1 );
		return sortedInputTuples;
	}
	
	public static JavaRDD<TuplePair> collectClosestPoints(JavaSparkContext context,
			List<Tuple> points) {
		JavaRDD<Tuple> pointsRDD = context.parallelize(points);
		final Broadcast<List<Tuple>> broadcastPoints = context
				.broadcast(pointsRDD.collect());

		JavaRDD<TuplePair> closestPointCollection = pointsRDD.map(new Function<Tuple, TuplePair>() {
			public TuplePair call(Tuple tuple1) {
				TuplePair closestPair = null;
				for (Tuple tuple2 : broadcastPoints.value()) {
					closestPair = getClosestPair(tuple1, tuple2, closestPair);
				}

				return closestPair;
			}
			
			private TuplePair getClosestPair(Tuple tuple1, Tuple tuple2,
					TuplePair closestPair) {
				if (tuple1.distance(tuple2) != 0 && (closestPair == null 
						|| tuple1.distance(tuple2) < closestPair.getDistance())) {
					closestPair = new TuplePair(tuple1, tuple2);
				}
				return closestPair;
			}
		});
		return closestPointCollection;
	}
	
	private static TuplePair reduceClosestPointCollection(JavaRDD<TuplePair> closestPointCollection) {
		TuplePair tuplePair = closestPointCollection.reduce(new Function2<TuplePair, TuplePair, TuplePair>() {
			public TuplePair call(TuplePair tuple1, TuplePair tuple2) {
				if (tuple1 == null)
					return tuple2;
				if (tuple2 == null)
					return tuple1;
				return tuple1.getDistance() < tuple2.getDistance() ? tuple1 : tuple2;
			}
		});
		return tuplePair;
	}
}
