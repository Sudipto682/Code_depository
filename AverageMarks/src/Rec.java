import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Rec implements Writable{
	
	private int count;
	private float avg;

	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public float getAvg() {
		return avg;
	}

	public void setAvg(float f) {
		this.avg = f;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		count=in.readInt();
		avg=in.readFloat();
		
		
	}

	@Override
	public void write(DataOutput d) throws IOException {
		// TODO Auto-generated method stub
		d.writeInt(count);
		d.writeFloat(avg);
	}

	@Override
	public String toString() {
		return "Rec [count=" + count + ", avg=" + avg + "]";
	}
	
	

}
