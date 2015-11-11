package edu.asu.cse512;

import java.util.ArrayList;
import java.util.List;


public class Polygon implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8487576571767008225L;
	List<Tuple> polygon;
	
	public Polygon(){
		polygon = new ArrayList<Tuple>();
	}
	
	public Polygon(List<Tuple> points){
		polygon = points;
	}
	
	public void addTuple(Tuple t){
		polygon.add(t);
	}
	
	public String toString(){
		String str = "";
		for(Tuple t: polygon){
			str += t.getX() +","+ t.getY() +"\n";
		}
		return str;
	}
	
}
