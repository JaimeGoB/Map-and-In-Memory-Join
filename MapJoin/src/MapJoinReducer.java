import org.apache.hadoop.io.IntWritable;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MapJoinReducer  extends Reducer<Text, IntWritable, Text, IntWritable>
{
    //  STR_1 Bangalore   [ {280} {560} {456}.......]
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context c)throws IOException, java.lang.InterruptedException
    {

	int totalRevenue = 0;
	
	Iterator<IntWritable> valuesIter = values.iterator();

	/* For each store location */
	
	while (valuesIter.hasNext())
	{
	   
	    int revenue = valuesIter.next().get();
	   
	    totalRevenue += revenue;
	}
	
		//write to output file store id, location and total revenue
		//STR_1 Bangalore 1200
	    c.write(key,new IntWritable(totalRevenue) );
	} 

    }   
