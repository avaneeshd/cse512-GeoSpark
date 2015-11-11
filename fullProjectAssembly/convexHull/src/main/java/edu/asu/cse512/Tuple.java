package edu.asu.cse512;
/**
 * 
 */


import java.util.Comparator;

/**
 * @author hduser
 *
 */
public class Tuple implements Comparable<Tuple>, java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1312396634854390416L;
	double x;
	double y;
	
	public Tuple(double x, double y){
		this.x = x;
		this.y = y;
	}
	
	public double getX() {
		return x;
	}
	
	public void setX(double x) {
		this.x = x;
	}
	
	public double getY() {
		return y;
	}
	
	public void setY(double y) {
		this.y = y;
	}
	
	public double distance(Tuple t2){
		double dist = 0.0;
		double diffX = (this.x - t2.x);
		double diffY = (this.y - t2.y);
		double sum = (diffX*diffX + diffY*diffY);
		dist = Math.abs(Math.sqrt(sum));
		return dist;
	}

	public int compareTo(Tuple t2) {
		if(this.x == t2.x && this.y == t2.y) 
			return 0;
		else 
			return 1;
	}
	
	public String toString(){
		return getX() + "," + getY();
	}
}
