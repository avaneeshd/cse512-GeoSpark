package edu.asu.cse512;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class Join {
	/*
	 * Main function, take two parameter as input, output
	 * 
	 * @param inputLocation 1
	 * 
	 * @param inputLocation 2
	 * 
	 * @param outputLocation
	 * 
	 * @param inputType
	 * 
	 */
	public static void main(String[] args) {

		if (args.length < 4) {
			System.out.println("JoinQuery expects atleast 4 arguments, inputLocation and outputLocation. Exiting..");
			return;
		}

		SparkConf conf = new SparkConf().setAppName("Group24-JoinQuery");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> input1 = context.textFile(args[0]);
		JavaRDD<String> input2 = context.textFile(args[1]);
		String output = args[2];
		String type = args[3];

		deleteFilesIfExists(args[2]);

		// check the type of input 1
		// And check the corresponding version of RDD
		JavaRDD<Tuple> pointRDD = null;
		JavaRDD<Rectangle> rectangleRDD = null;
		if (type.equalsIgnoreCase("point")) {
			pointRDD = processInput1Point(input1);
		} else {
			rectangleRDD = processInput1Rectangle(input1);
		}
		JavaRDD<Rectangle> queryRDD = processInput2(input2);
		JavaRDD<Tuple2<Integer, ArrayList>> resultpointRDD = null;

		if (type.equalsIgnoreCase("point")) {
			resultpointRDD = processPointQuery(pointRDD, queryRDD);
		} else {
			resultpointRDD = processRectangleQuery(rectangleRDD, queryRDD);
		}

		JavaRDD<String> resultStringRDD = resultpointRDD.map(new Function<Tuple2<Integer, ArrayList>, String>() {

			public String call(Tuple2<Integer, ArrayList> tup) throws Exception {
				// TODO Auto-generated method stub
				String temp = "";
				temp += String.valueOf(tup._1());
				ArrayList<Integer> tempList = tup._2();
				if (tempList.size() == 0) {
					temp += "NULL";
				} else {
					for (Integer a : tempList)
						temp += "," + String.valueOf(a);
				}
				return temp;

			}

		});
		resultStringRDD.repartition(1).saveAsTextFile(output);

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

	private static JavaRDD<Tuple2<Integer, ArrayList>> processRectangleQuery(JavaRDD<Rectangle> rectangleRDD,
			JavaRDD<Rectangle> queryRDD) {

		final ArrayList<Integer> filteredRectangles = new ArrayList<Integer>();
		final List<Rectangle> rectangles = rectangleRDD.collect();

		JavaRDD<Tuple2<Integer, ArrayList>> resultRDD = queryRDD
				.map(new Function<Rectangle, Tuple2<Integer, ArrayList>>() {

					public Tuple2<Integer, ArrayList> call(Rectangle query) throws Exception {
						// TODO Auto-generated method stub

						for (Rectangle rectangle : rectangles) {
							if (query.checkRectanlgeInsideRectangle(rectangle) || query.checkOverlap(rectangle)) {
								if (!filteredRectangles.contains(rectangle.getRectangleID()))
									filteredRectangles.add(rectangle.getRectangleID());
							}
						}
						Collections.sort(filteredRectangles);
						return new Tuple2<Integer, ArrayList>(query.getRectangleID(), filteredRectangles);
					}

				});
		return resultRDD;

	}

	private static JavaRDD<Tuple2<Integer, ArrayList>> processPointQuery(JavaRDD<Tuple> pointRDD,
			JavaRDD<Rectangle> queryRDD) {
		final ArrayList<Integer> filteredPoints = new ArrayList<Integer>();
		final List<Tuple> points = pointRDD.collect();

		JavaRDD<Tuple2<Integer, ArrayList>> resultRDD = queryRDD
				.map(new Function<Rectangle, Tuple2<Integer, ArrayList>>() {

					public Tuple2<Integer, ArrayList> call(Rectangle query) throws Exception {
						// TODO Auto-generated method stub

						for (Tuple point : points) {
							if (query.checkPointInsideRectangle(point)) {
								if (!filteredPoints.contains(point.getId()))
									filteredPoints.add(point.getId());
							}
						}

						Collections.sort(filteredPoints);
						return new Tuple2<Integer, ArrayList>(query.getRectangleID(), filteredPoints);

					}

				});

		return resultRDD;
	}

	private static JavaRDD<Rectangle> processInput2(JavaRDD<String> input2) {
		// TODO Auto-generated method stub
		JavaRDD<Rectangle> queryRDD = input2.map(new Function<String, Rectangle>() {

			public Rectangle call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] coordinates = line.split(",");
				int id = Integer.valueOf(coordinates[0]);
				double x = Double.valueOf(coordinates[1]);
				double y = Double.valueOf(coordinates[2]);
				double p = Double.valueOf(coordinates[3]);
				double q = Double.valueOf(coordinates[4]);
				double minX = (x < p) ? x : p;
				double minY = (y < q) ? y : q;
				double maxX = (x > p) ? x : p;
				double maxY = (y > q) ? y : q;
				Tuple topleft = new Tuple(minX, minY);
				Tuple bottomright = new Tuple(maxX, maxY);
				Rectangle rect = new Rectangle(id, topleft, bottomright);
				return rect;
			}

		});
		return queryRDD;
	}

	private static JavaRDD<Rectangle> processInput1Rectangle(JavaRDD<String> input1) {
		JavaRDD<Rectangle> rectangleRDD = input1.map(new Function<String, Rectangle>() {

			public Rectangle call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] coordinates = line.split(",");
				int id = Integer.valueOf(coordinates[0]);
				double x = Double.valueOf(coordinates[1]);
				double y = Double.valueOf(coordinates[2]);
				double p = Double.valueOf(coordinates[3]);
				double q = Double.valueOf(coordinates[4]);
				double minX = (x < p) ? x : p;
				double minY = (y < q) ? y : q;
				double maxX = (x > p) ? x : p;
				double maxY = (y > q) ? y : q;
				Tuple topleft = new Tuple(minX, minY);
				Tuple bottomright = new Tuple(maxX, maxY);
				Rectangle rect = new Rectangle(id, topleft, bottomright);
				return rect;
			}

		});
		return rectangleRDD;
	}

	private static JavaRDD<Tuple> processInput1Point(JavaRDD<String> input1) {
		JavaRDD<Tuple> tupleRDD = input1.map(new Function<String, Tuple>() {

			public Tuple call(String line) throws Exception {
				String[] coordinates = line.split(",");
				int id = Integer.valueOf(coordinates[0]);
				double x = Double.valueOf(coordinates[1]);
				double y = Double.valueOf(coordinates[2]);

				Tuple tup = new Tuple(x, y);
				tup.setId(id);
				return tup;
			}
		});

		return tupleRDD;
	}

}
