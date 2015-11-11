package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TuplePair implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Tuple tuple1, tuple2;

	private double distance;

	public TuplePair(Tuple tuple1, Tuple tuple2) {
		super();
		this.tuple1 = tuple1;
		this.tuple2 = tuple2;
		this.distance = tuple1.distance(tuple2);
	}

	public Tuple getTuple1() {
		return tuple1;
	}

	public Tuple getTuple2() {
		return tuple2;
	}

	public double getDistance() {
		return distance;
	}
	
	public String toString() {
		String tuple1String = getTuple1().getX() + "," + getTuple1().getY();
		String tuple2String = getTuple2().getX() + "," + getTuple2().getY();
		return tuple1String + "\n" + tuple2String;
	}
	
	public List<Tuple> asList() {
		List<Tuple> pairList = new ArrayList<Tuple>();
		if (tuple1 != null)
			pairList.add(tuple1);
		if (tuple2 != null)
			pairList.add(tuple2);
		return pairList;
	}

}
