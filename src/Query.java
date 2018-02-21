/*  Member
 *  
 *  1. Thanapon Jarukasetphon  		5888057 	Section: 1
 *  2. Papatsapong Jiraroj-ungkun 	5888060		Section: 1
 *  3. Chatchanin  Yimudom  		5888234		Section: 1
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Query {

	// Term id -> position in index file
	private  Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private  Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private  Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private  Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private  BaseIndex index = null;
	

	//indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;
	
	/* 
	 * Read a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private  PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
		/*
		 * TODO: Your code here
		 */
		//Find the position of term in corpus
		fc.position(posDict.get(termId));
		
		PostingList p =index.readPosting(fc);
		return p;
	}
	
	
	public void runQueryService(String indexMode, String indexDirname) throws IOException
	{
		//Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode+"Index");
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		//Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			return;
		}
		
		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname,
				"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
				indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
				indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
				indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
					Integer.parseInt(tokens[2]));
		}
		postReader.close();
		
		this.running = true;
	}
    
	public List<Integer> retrieve(String query) throws IOException
	{	if(!running) 
		{
			System.err.println("Error: Query service must be initiated");
		}
		
		/*
		 * TODO: Your code here
		 *       Perform query processing with the inverted index.
		 *       return the list of IDs of the documents that match the query
		 *      
		 */
		//List used to store intersect for each loop
		List<Integer> intersect = new ArrayList<Integer>();
		//List used to store previous intersection result
		List<Integer> prevIntersect = new ArrayList<Integer>();
		String[] tok = query.split("\\s+");
		int size=0,valC,valP,loopcount=0;//Loopcount for number of new terms encountered
		PostingList cur;
		for(int i=0;i<tok.length;i++)
		{
			//Check if the term exists or not
			if(termDict.containsKey(tok[i]))
			{
				if(size==0)//First Term Encountered
				{
					cur = readPosting(indexFile.getChannel(),termDict.get(tok[i]));
					prevIntersect = cur.getList();
					loopcount++;
				}
				else if(size==1)//Second Terms encountered onwards
				{
					//Update current postinglist
					cur = readPosting(indexFile.getChannel(),termDict.get(tok[i]));
					//Current PostingList
					List<Integer> curList = cur.getList();
					//Previous Intersection result
					List<Integer> prevList = prevIntersect;
					int cursize = curList.size();
					int prevsize = prevList.size();
					int indexC=0,indexP=0;
					//Clear the previous result
					intersect.clear();
					while(indexC<cursize&&indexP<prevsize)
					{
						valC=curList.get(indexC);
						valP=prevList.get(indexP);
						if(valC==valP)//Add when both of them are equal
						{
							intersect.add(valC);
							indexC++;
							indexP++;
						}
						else if(valC<valP)//Move index in current list when document id in current list is lesser
						{
							indexC++;
						}
						else//Move index in previous list when document id in previous list is lesser
						{
							indexP++;
						}
					}
					loopcount++;
					//Clear and update result
					prevIntersect.clear();
					prevIntersect.addAll(intersect);
					size--;
				}
				size++;
			}
			else
				return null;
		}
		if(intersect.size()==0&&loopcount==0) return null;//When no terms in query exist in termDict
		else if(loopcount==1) return prevIntersect;//Only 1 term in query
		return intersect;
	}
	
    String outputQueryResult(List<Integer> res) {
        /*
         * TODO: 
         * 
         * Take the list of documents ID and prepare the search results, sorted by lexicon order. 
         * 
         * E.g.
         * 	0/fine.txt
		 *	0/hello.txt
		 *	1/bye.txt
		 *	2/fine.txt
		 *	2/hello.txt
		 *
		 * If there no matched document, output:
		 * 
		 * no results found
		 * 
         * */
    	if(res!=null)//If the list returned from query service is not null
    	{
    		StringBuilder message = new StringBuilder();
    		for(Integer i:res)
    		{
    			message.append(docDict.get(i)+"\n");
    		}
    		return message.toString();
    	}
    	return "no results found\n";
    }
	
	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = null;
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];
		
		Query queryService = new Query();
		queryService.runQueryService(className, args[1]);
		
		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}
		
		br.close();
	}
	
	protected void finalize()
	{
		try {
			if(indexFile != null)indexFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

