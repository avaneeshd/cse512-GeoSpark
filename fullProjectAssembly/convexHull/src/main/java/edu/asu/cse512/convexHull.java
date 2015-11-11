package edu.asu.cse512;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class convexHull implements java.io.Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5797623354794746176L;
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	 * OUTPUT:
	 * set of tuples written to given output file
	*/
	Polygon convex_hull = null;
	String input = "";
	String output ="";
    public static void main( String[] args )
    {
    	if(args.length < 2){
    		System.out.println("Convex Hull expects atleast 2 arguments, inputLocation and outputLocation. Exiting..");
    		return;
    	}
    	//Get spark context
    	SparkConf  conf  =  new  SparkConf (). setAppName ( "Group24-ConvexHull" );  
    	JavaSparkContext  context  =  new  JavaSparkContext ( conf );
    	//Initialize convex hull
    	convexHull cHull = new convexHull();
    	
    	//TODO: Initialize, need to remove existing in output file location.
    	//cHull.deleteFilesIfExists();
    	
    	cHull.input = args[0];
    	cHull.output = args[1];
    	cHull.deleteFilesIfExists(cHull.output);
    	//Run convex hull
    	cHull.run(context);
    }
    
    private void run(JavaSparkContext  context){
    	System.out.println("Running Convex Hull");
    	JavaRDD<String> inputRDD = context.textFile(input);
        JavaRDD<Tuple> inputTuples = processInput(inputRDD);
		JavaRDD<List<Tuple>> inputListRDD = inputTuples.glom();
		
		//Mapping Phase
		JavaRDD<List<Tuple>> hullRDD = inputListRDD.mapPartitions(quickHull);
		//Reduce Phase
		List<Tuple> finalHull = hullRDD.reduce(reduceHull);
		JavaRDD<Tuple> convexHullRDD = context.parallelize(finalHull).repartition(1);
		
		//Create a polygon
		convex_hull = new Polygon();
		convex_hull.polygon = convexHullRDD.collect();
		
		//Sort the final list
		Collections.sort(convex_hull.polygon, new Comparator<Tuple>() { 
			public int compare(Tuple o1, Tuple o2) {
				int result = Double.compare(o1.x, o2.x); 
				return (result == 0 ? Double.compare(o1.y, o2.y) : result); 
			}
		});
		JavaRDD<Tuple> finalHullRDD = context.parallelize(convex_hull.polygon).repartition(1);
		
		//Write result to file 
		System.out.println("Saving results to the output file");
		finalHullRDD.saveAsTextFile(output);		
    }
    
    
    private void deleteFilesIfExists(String outputPath){
    	//Delete any output files if present
    	Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create(outputPath), conf);
	        hdfs.delete(new Path(output), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
    }
    	private JavaRDD<Tuple> processInput(JavaRDD<String> inputRDD){
    		/**
    		 * Process input strings and map them to Tuples
    		 */
    		JavaRDD<Tuple> pointsRDD = inputRDD.map(new Function<String, Tuple>(){

    			private static final long serialVersionUID = 7373617044676146795L;

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

    	public Polygon getConvexHull(){
    		/**
    		 * Returns convex hull
    		 */
    		return convex_hull;
    	}
    	
    	private final FlatMapFunction<Iterator<List<Tuple>>, List<Tuple>> quickHull = new FlatMapFunction<Iterator<List<Tuple>>, List<Tuple>>(){
    		/**
    		 *  Mapper function to determine convex hull for each partition
    		 */
    		private static final long serialVersionUID = -4938677865060358964L;

    		private int tupleLocation(Tuple A, Tuple B, Tuple P){
    			// Tuple A and B represents end points of a line
    			// Test tuple P for location on right or left of line AB
    			double cp1 = (B.x - A.x) * (P.y - A.y) - (B.y - A.y) * (P.x - A.x);

    			if(cp1 > 0)
    				return 1;
    			else if(cp1 == 0)
    				return 0;
    			else
    				return -1;
    		}
    	private double distanceFromLine(Tuple A, Tuple B, Tuple P){
			//Find distance of point P from line AB
			double ABx = B.x - A.x;
			double ABy= B.y - A.y;

			double num = (ABx * (A.y - P.y) )- (ABy * (A.x - P.x));
			if ( num < 0)
				num = -num;
			return num;
		}

		private ArrayList<Tuple> findMinMax(ArrayList<Tuple> tuples){
			/**
			 * Returns minimum and maximum tuples in the list by x coordinate
			 */
			ArrayList<Tuple> minmax = new ArrayList<Tuple>();
			minmax.add(tuples.get(0));
			minmax.add(tuples.get(0));

			double maxX = Double.MIN_VALUE;
			double minX = Double.MAX_VALUE;
			for(Tuple tuple: tuples){
				if(tuple.getX() < minX){
					minX = tuple.getX();
					minmax.set(0, tuple);
				}

				if(tuple.getX() > maxX){
					maxX = tuple.getX();
					minmax.set(1, tuple);
				}
			}
			return minmax;
		}

		private ArrayList<ArrayList<Tuple>> findTupleSets(Tuple min, Tuple max, ArrayList<Tuple> tuples){
			/*
			 *  Finds left set and right set from tuples
			 * */
			ArrayList<ArrayList<Tuple>> ret = new ArrayList<ArrayList<Tuple>>();
			ArrayList<Tuple> leftSet = new ArrayList<Tuple>();
			ArrayList<Tuple> rightSet = new ArrayList<Tuple>();
			for(Tuple t : tuples){
				if(tupleLocation(min, max, t) == -1)
					leftSet.add(t);
				else if(tupleLocation(min, max, t) == 1)
					rightSet.add(t);
			}
			ret.add(leftSet);
			ret.add(rightSet);
			return ret;
		}

		private void findHull(Tuple A, Tuple B, Polygon hull, ArrayList<Tuple> set){
			int pos = hull.polygon.indexOf(B);
			if (set.size() == 0)
				return;
			if (set.size() == 1)
			{
				Tuple p = set.get(0);
				set.remove(p);
				hull.polygon.add(pos,p);
				return;
			}

			Tuple farthestPoint = set.get(0);
			double distance = Double.MIN_VALUE;
			for(Tuple t: set){
				double d = distanceFromLine(A, B, t);
				if(d > distance){
					distance = d;
					farthestPoint = t;
				}
			}

			set.remove(farthestPoint);
			hull.polygon.add(pos,farthestPoint);

			ArrayList<Tuple> leftSetAP = new ArrayList<Tuple>();
			for(Tuple t : set){
				if(tupleLocation(A, farthestPoint, t) == 1){
					leftSetAP.add(t);
				}
			}

			ArrayList<Tuple> leftSetPB = new ArrayList<Tuple>();
			for(Tuple t : set){
				if(tupleLocation(farthestPoint, B, t) == 1){
					leftSetPB.add(t);
				}
			}

			findHull(A, farthestPoint, hull,leftSetAP);
			findHull(farthestPoint, B, hull, leftSetPB);

		}

		public Iterable<List<Tuple>> call(Iterator<List<Tuple>> iter) throws Exception {
			Polygon convexHull = new Polygon();
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			while(iter.hasNext()){
				tuples.addAll(iter.next());
			}
			//Finds minimum and maximum X coordinates
			ArrayList<Tuple> minmax = findMinMax(tuples);

			convexHull.addTuple(minmax.get(0));
			convexHull.addTuple(minmax.get(1));

			//remove from the set
			tuples.remove(minmax.get(0));
			tuples.remove(minmax.get(1));

			//Find left and right set
			ArrayList<ArrayList<Tuple>> sets = findTupleSets(minmax.get(0), minmax.get(1), tuples);

			//Recursively call findHull
			findHull(minmax.get(0), minmax.get(1), convexHull, sets.get(1));
			findHull(minmax.get(1), minmax.get(0), convexHull, sets.get(0));

			//Return ConvexHull
			ArrayList<List<Tuple>> result = new ArrayList<List<Tuple>>();
			result.add(convexHull.polygon);
			return result;
		}
	};
	
	
	final Function2<List<Tuple>, List<Tuple>, List<Tuple>> reduceHull = new Function2<List<Tuple>, List<Tuple>, List<Tuple>>(){
		private static final long serialVersionUID = 1L;

		public List<Tuple> call(List<Tuple> hull1, List<Tuple> hull2) throws Exception {
			// Combine the tuples of two lists
			hull1.addAll(hull2);
			
			//Map from Tuple to Coordinate 
			GeometryFactory geom = new GeometryFactory();
			List<Coordinate> ActiveCoord = new ArrayList<Coordinate>();
			for(Tuple t : hull1){
				Coordinate co = new Coordinate(t.x, t.y);
				ActiveCoord.add(co);
			}
			
			// Run ConvexHull on the set of coordinates
			com.vividsolutions.jts.algorithm.ConvexHull ch = new com.vividsolutions.jts.algorithm.ConvexHull(ActiveCoord.toArray(new Coordinate[ActiveCoord.size()]), geom);
			Geometry g=ch.getConvexHull();
			Coordinate[] c= g.getCoordinates();
			List<Tuple> finalHull = new ArrayList<Tuple>();
			List<Coordinate> cArrayList = Arrays.asList(c).subList(0, c.length-1);
			c = cArrayList.toArray(new Coordinate[cArrayList.size()]);
			
			//Map from Coordinate to Tuple
			for(int i=0; i<c.length; i++){
				Tuple t = new Tuple(c[i].x, c[i].y);
				finalHull.add(t);
			}
			//Return final list
			return finalHull;
		}
	};
}
