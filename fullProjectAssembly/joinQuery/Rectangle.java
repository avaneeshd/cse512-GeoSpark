package asu.edu.dds;

public class Rectangle implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4124164241693231547L;
	int rectangleID;
	Tuple topleft;
	Tuple bottomright;

	public Rectangle(int rectanlgeID, Tuple topleft, Tuple bottomright) {
		this.rectangleID = rectanlgeID;
		this.topleft = topleft;
		this.bottomright = bottomright;
	}

	public int getRectangleID() {
		return rectangleID;
	}

	public void setRectangleID(int rectangleID) {
		this.rectangleID = rectangleID;
	}

	public Tuple getTopleft() {
		return topleft;
	}

	public void setTopleft(Tuple topleft) {
		this.topleft = topleft;
	}

	public Tuple getBottomright() {
		return bottomright;
	}

	public void setBottomright(Tuple bottomright) {
		this.bottomright = bottomright;
	}

	public Boolean checkPointInsideRectangle(Tuple point) {
		return (point.getX() >= getTopleft().getX() && point.getY() >= getTopleft().getY()
				&& point.getX() <= getBottomright().getX() && point.getY() <= getBottomright().getY());
	}

	public Boolean checkRectanlgeInsideRectangle(Rectangle rect) {
		return (checkPointInsideRectangle(new Tuple(rect.getTopleft().getX(), rect.getTopleft().getY()))
				&& checkPointInsideRectangle(new Tuple(rect.getTopleft().getX(), rect.getBottomright().getY()))
				&& checkPointInsideRectangle(new Tuple(rect.getBottomright().getX(), rect.getBottomright().getY()))
				&& checkPointInsideRectangle(new Tuple(rect.getBottomright().getX(), rect.getTopleft().getY())));
	}

	public Boolean checkOverlap(Rectangle rect) {
		if (this.getTopleft().getX() > rect.getBottomright().getX()
				|| rect.getTopleft().getX() > this.getBottomright().getX())
			return false;

		if (this.getTopleft().getY() > rect.getBottomright().getY()
				|| rect.getTopleft().getY() >  this.getBottomright().getY())
			return false;
		
		return  true;
	}

	public String toString() {
		return getRectangleID() + "," + getTopleft().getX() + "," + getTopleft().getY() + "," + getBottomright().getX()
				+ "," + getBottomright().getY();
	}
}
