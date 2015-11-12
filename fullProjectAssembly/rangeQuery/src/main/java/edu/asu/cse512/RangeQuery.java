package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

/**
 * Hello world!
 *
 */
@SuppressWarnings("serial")
public class RangeQuery implements Serializable {
	/*
	 * Main function, take two parameter as input, output
	 * 
	 * @param inputLocation
	 * 
	 * @param outputLocation
	 * 
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Range Query expects atleast 3 arguments, input1, input2 and Ouputlocation. Exiting..");
			return;
		}

		SparkConf conf = new SparkConf().setAppName("RangQuery");
		JavaSparkContext context = new JavaSparkContext(conf);
		deleteFilesIfExists(args[2]);
		try {
			JavaRDD<String> pointsRDD = context.textFile(args[0]);
			JavaRDD<String> queryRDD = context.textFile(args[1]);
			rangeQuery(pointsRDD, queryRDD, args[2]);
		} catch (Exception ex) {
			System.out.println("Exception occured processing the input" + ex.getMessage());
		}
		// Initialize, need to remove existing in output file location.

		// Implement

		// Output your result, you need to sort your result!!!
		// And,Don't add a additional clean up step delete the new generated
		// file...
	}

	private static void deleteFilesIfExists(String outputPath) {
		// Delete any output files if present
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create(outputPath), conf);
			hdfs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void rangeQuery(JavaRDD<String> input1, JavaRDD<String> input2, String output) {
		JavaRDD<HashMap<Integer, Tuple>> pointsRDD = processInput1(input1);
		JavaRDD<Polygon> queryRDD = processInput2(input2);
		// Take the 1st query
		final Polygon query = queryRDD.first();

		// Filter the points inside the query
		JavaRDD<HashMap<Integer, Tuple>> resultRDD = pointsRDD.filter(new Function<HashMap<Integer, Tuple>, Boolean>() {
			public Boolean call(HashMap<Integer, Tuple> point) {
				for (Entry<Integer, Tuple> e : point.entrySet()) {
					Tuple topleft = query.polygon.get(0);
					Tuple bottomright = query.polygon.get(1);
					if (e.getValue().getX() > topleft.getX() && e.getValue().getY() > topleft.getY()
							&& e.getValue().getX() < bottomright.getX() && e.getValue().getY() < bottomright.getY()) {
						return true;
					}
				}
				return false;
			}
		});

		// Convert HashMapRDD to Integer RDD
		JavaRDD<Integer> keysRDD = getKeys(resultRDD);
		keysRDD.repartition(1).saveAsTextFile(output);
	}

	private static JavaRDD<Integer> getKeys(JavaRDD<HashMap<Integer, Tuple>> filteredPoints) {
		JavaRDD<Integer> idRDD = filteredPoints.flatMap(new FlatMapFunction<HashMap<Integer, Tuple>, Integer>() {
			public Iterable<Integer> call(HashMap<Integer, Tuple> t) throws Exception {
				ArrayList<Integer> tempList = new ArrayList<Integer>(t.keySet());
				Collections.sort(tempList);
				return tempList;
			}
		});
		return idRDD;
	}

	private static JavaRDD<Polygon> processInput2(JavaRDD<String> input2) {
		JavaRDD<Polygon> polygonRDD = input2.map(new Function<String, Polygon>() {

			public Polygon call(String line) throws Exception {
				String[] coordinates = line.split(",");
				double x = Double.valueOf(coordinates[0]);
				double y = Double.valueOf(coordinates[1]);
				double p = Double.valueOf(coordinates[2]);
				double q = Double.valueOf(coordinates[3]);
				Tuple one = new Tuple(x, y);
				Tuple two = new Tuple(p, q);
				Polygon polygon = new Polygon();
				polygon.addTuple(one);
				polygon.addTuple(two);
				return polygon;
			}
		});

		return polygonRDD;
	}

	private static JavaRDD<HashMap<Integer, Tuple>> processInput1(JavaRDD<String> input1) {
		JavaRDD<HashMap<Integer, Tuple>> pointsRDD = input1.map(new Function<String, HashMap<Integer, Tuple>>() {

			public HashMap<Integer, Tuple> call(String line) throws Exception {
				// TODO Auto-generated method stub
				String coordinates[] = line.split(",");
				int id = Integer.valueOf(coordinates[0]);
				double x = Double.valueOf(coordinates[1]);
				double y = Double.valueOf(coordinates[2]);
				Tuple tup = new Tuple(x, y);
				HashMap<Integer, Tuple> map = new HashMap<Integer, Tuple>();
				map.put(id, tup);
				return map;
			}

		});
		return pointsRDD;
	}
}
