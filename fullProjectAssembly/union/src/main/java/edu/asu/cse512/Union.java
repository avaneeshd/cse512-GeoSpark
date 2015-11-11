package edu.asu.cse512;

import java.io.IOException;
import java.io.Serializable;
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
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

/**
 * Hello world!
 *
 */
@SuppressWarnings("serial")
public class Union implements Serializable
{
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
    public static void main( String[] args )
    {
    	if(args.length < 2){
    		System.out.println("Convex Hull expects atleast 2 arguments, inputLocation and outputLocation. Exiting..");
    		return;
    	}
    	
    	SparkConf conf = new SparkConf().setAppName("GeometryUnion");
        JavaSparkContext context = new JavaSparkContext(conf);
        //Initialize, need to remove existing in output file location.
    	
    	//Implement 
    	
        try {
        	JavaRDD<String> inputRDD = context.textFile(args[0]);
        	geometryUnion(inputRDD, context, args[1]);
        } catch (Exception ex) {
        	System.out.println("Exception occured processing the input" + ex.getMessage());
        }
    }
    
    /**
    *
    * @param inputRDD
    * @param outputPath
    * @throws Exception
    *             Algorithm: Step 1 : Input lines are mapped to Polygons Step 2
    *             : These Polygons are mapped to Geometry Objects(Geometry
    *             Objects are used for use of JTS Library ) and their unions
    *             are computed distributedly.Cascaded union is used for faster
    *             access. Step 3: These results are moved back to Master and
    *             their union is computed by the reducer for final union
    */

   public static void geometryUnion(JavaRDD<String> inputRDD, JavaSparkContext context,
           String outputPath) throws Exception {

       JavaRDD<Polygon> inputPolygons = inputRDD.map(tupleToPolygon);
       JavaRDD<Geometry> geometryPolygons = inputPolygons
               .mapPartitions(polygonToGeometry);
       Geometry result = geometryPolygons.reduce(computeUnion);
       List<Coordinate> points = Arrays.asList(result.getCoordinates());

       // Last point is deleted as Geometry Objects are represented as closed
       // form representation of points. So first point is same as last point.
       points = points.subList(0, points.size() - 1);

       Collections.sort(points, new Comparator<Coordinate>() {
           public int compare(Coordinate o1, Coordinate o2) {
               int result = Double.compare(o1.x, o2.x);
               return (result == 0 ? Double.compare(o1.y, o2.y) : result);
           }
       });

       List<Tuple> finalPoints = new ArrayList<Tuple>();
       for (Coordinate eachPoint : points) {

           finalPoints.add(new Tuple(eachPoint.x, eachPoint.y));
       }

       JavaRDD<Tuple> result1 = context.parallelize(finalPoints)
               .repartition(1);
       result1.saveAsTextFile(outputPath);
   }
   
   private static void deleteFilesIfExists(String outputPath) {
   	//Delete any output files if present
   	Configuration conf = new Configuration();
       conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
       conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
       FileSystem hdfs;
		try {
			hdfs = FileSystem.get(URI.create("hdfs://<namenode-hostname>:<port>"), conf);
	        hdfs.delete(new Path(outputPath), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
   }
   /**
    * Mapper for converting from inputTuples to Polygons
    */

   public final static Function<String, Polygon> tupleToPolygon = new Function<String, Polygon>() {

       public Polygon call(String v1) throws Exception {

           String[] coordinates = v1.split(",");
           // Assuming in this case the only possible type of polygon is
           // Rectangle.
           Tuple leftTopPoint = new Tuple(Double.parseDouble(coordinates[0]),
                   Double.parseDouble(coordinates[1]));
           Tuple rightBottomPoint = new Tuple(
                   Double.parseDouble(coordinates[2]),
                   Double.parseDouble(coordinates[3]));

           Polygon polygon = new Polygon();
           polygon.addTuple(leftTopPoint);
           polygon.addTuple(rightBottomPoint);

           return polygon;
       }
   };

   /**
    * Mapper for converting from Polygon to Geometry Object Geometry object is
    * necessary for the use of JTS Library
    */
   private final static FlatMapFunction<Iterator<Polygon>, Geometry> polygonToGeometry = new FlatMapFunction<Iterator<Polygon>, Geometry>() {

       List<Geometry> polygonsToUnion = new ArrayList<Geometry>();

       public Iterable<Geometry> call(Iterator<Polygon> polygon)
               throws Exception {
           GeometryFactory fact = new GeometryFactory();
           // Four points specifying the rectangles
           while (polygon.hasNext()) {
               Polygon eachPolygon = polygon.next();
               Geometry geometry = fact.createPolygon(new Coordinate[] {
                       new Coordinate(eachPolygon.polygon.get(0).x,
                               eachPolygon.polygon.get(0).y),
                       new Coordinate(eachPolygon.polygon.get(1).x,
                               eachPolygon.polygon.get(0).y),
                       new Coordinate(eachPolygon.polygon.get(1).x,
                               eachPolygon.polygon.get(1).y),
                       new Coordinate(eachPolygon.polygon.get(0).x,
                               eachPolygon.polygon.get(1).y),
                       new Coordinate(eachPolygon.polygon.get(0).x,
                               eachPolygon.polygon.get(0).y) });
               polygonsToUnion.add(geometry);
           }

           CascadedPolygonUnion cascadedPolygonUnionResult = new CascadedPolygonUnion(
                   polygonsToUnion);

           Geometry gPolResult = cascadedPolygonUnionResult.union();
           List<Geometry> returnPolygons = new ArrayList<Geometry>();
           returnPolygons.add(gPolResult);
           return returnPolygons;
       }
   };

   /**
    * Reducer which uses JTS Library to calculate union
    */

   private static Function2<Geometry, Geometry, Geometry> computeUnion = new Function2<Geometry, Geometry, Geometry>() {

       public Geometry call(Geometry rectangleOne, Geometry rectangleTwo)
               throws Exception {
           List<Geometry> polygonsToUnion = new ArrayList<Geometry>();
           polygonsToUnion.add(rectangleOne);
           polygonsToUnion.add(rectangleTwo);
           CascadedPolygonUnion cascadedPolygonUnionResult = new CascadedPolygonUnion(
                   polygonsToUnion);

           return cascadedPolygonUnionResult.union();
       }

   };

}
