
import java.util.HashMap;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	//use to store - store file
                                                    // key: store_id  value: store_location
    private HashMap<String, String> stores = new HashMap<String, String>();                   // [ {STR_1:Bangalore} ]
	
    //use to store - product file
                                            //key: product_id  value: product_price
    private HashMap<String, String> products = new HashMap<String, String>();               // [{ PR_1:40}
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
    	
	BufferedReader br = null;
	//to store location of two files
	
	Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	//Path[] cacheFilesLocal = context.getLocalCacheFiles();

	String line = "";
	
	//Iterate through locations to get files
	for (Path path : cacheFilesLocal)
	{
		
	    if (path.getName().toString().trim().equals("store.txt")) 
	    {

	      br = new BufferedReader(new FileReader(path.toString()));
	      line=br.readLine();
	      
		    while (line!= null)
		    {
		    	//line = "STR_1,Bangalore,Walmart"
		    	String storeData[] = line.split(",");        
		    	//storeData = [STR_1  Bangalore  Walmart]
		    	
		    	//str_1 , Bangalore
		    	stores.put(storeData[0].trim(), storeData[1].trim());
		    	line=br.readLine();
		    }
		} else if (path.getName().toString().trim().equals("product.txt")){
		    
			br = new BufferedReader(new FileReader(path.toString()));
		    //PR_1,Shoes,Sport,40
		    line=br.readLine();
		      
			    while (line!= null)
			    {
			    	//productData =[ PR_1 Shoes Sport 40]
			    	String productData[] = line.split(",");     
			    	//PR_1, 40
			    	products.put(productData[0].trim(), productData[3].trim());
			    	line=br.readLine();
			    }
			} 
		}	
	}    

    @Override
    protected void map(LongWritable key, Text value,  Context context)throws IOException, java.lang.InterruptedException
    {

    //Will read first value from sales file
    //STR_1,PR_5,2009-12-29 08:31:21,7
	String line = value.toString();
	
	/* words = [STR_1 PR_1 06:09:01 7] */
	String[] words = line.split(",");     

	//storeID = STR_1
	String storeID= words[0];
	
	//productSale = 7
	int productSale = Integer.parseInt(words[3].trim());
	
	
	//productPrice = 40
	int productPrice = Integer.parseInt(products.get(words[1]));
	/*
	 * products:
	 * PR_1, 40
	 * 
	 * store:
	 * str_1 , Bangalore 
	 * 
	 * words
	 * [STR_1 PR_1 06:09:01 7]
	 * 
	 * */
	
	//revenue = 7 * 40
    int revenue = 	productSale*productPrice;
    
    //Bangalore
    String location = stores.get(storeID.toString());
    
    //output of mapper will be store id, location and revenue.
    //STR_1 Bangalore 280 
	context.write(new Text ((storeID)+" " + location), new IntWritable (revenue));
    }
}
