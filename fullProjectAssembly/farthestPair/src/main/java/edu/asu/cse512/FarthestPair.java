package edu.asu.cse512;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FarthestPair implements java.io.Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 805145188888439028L;
	/*
	 * Main function, take two parameter as input, output
	 * @param inputLocation
	 * @param outputLocation
	 * 
	*/
	String input = "";
	String output = "";
    public static void main( String[] args )
    {
        //Initialize, need to remove existing in output file location.
    	
    	//Implement 
    	if(args.length < 2){
    		System.out.println("Convex Hull expects atleast 2 arguments, inputLocation and outputLocation. Exiting..");
    		return;
    	}
    	//Get spark context
    	SparkConf  conf  =  new  SparkConf (). setAppName ( "Group24-FarthestPair" );  
    	JavaSparkContext  context  =  new  JavaSparkContext ( conf );
    	
    	//Initialize convex hull
    	FarthestPair fPair = new FarthestPair();
    	
    	//TODO: Initialize, need to remove existing in output file location.
    	//cHull.deleteFilesIfExists();
    	
    	fPair.input = args[0];
    	fPair.output = args[1];
    	
    	//Run FarthestPair
    	fPair.run(context);
    }
    
    private void run(JavaSparkContext context){

   	 	JavaRDD<String> inputRDD = context.textFile(input);
   	 
    	convexHull convexHull = new convexHull(context, inputRDD, output);
		Polygon hull = convexHull.getConvexHull();
		
		ArrayList<Tuple> farthestPair = findFarthestPair(hull);
		JavaRDD<Tuple> result = context.parallelize(farthestPair).repartition(1);

		result.saveAsTextFile(output);
    }
    
	private ArrayList<Tuple> findFarthestPair(Polygon polygon){
		/**
		 * 
		 */
		ArrayList<Tuple> pair = new ArrayList<Tuple>();
		Tuple farthest1 = polygon.polygon.get(0) , farthest2 = polygon.polygon.get(0);
		double maxDist = Double.MIN_VALUE;
		
		for(Tuple t1: polygon.polygon){
			for(Tuple t2: polygon.polygon){
				if(t1.distance(t2) > maxDist){
					maxDist = t1.distance(t2);
					farthest1 = t1;
					farthest2 = t2;
				}
			}
		}
	
		pair.add(farthest1);
		pair.add(farthest2);
		
		return pair;
		
	}
}
