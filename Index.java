/*  Member
 *  
 *  1. Thanapon Jarukasetphon  		5888057 	Section: 1
 *  2. Papatsapong Jiraroj-ungkun 	5888060		Section: 1
 *  3. Chatchanin Yimudom  			5888234		Section: 1
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list to the given file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * */
	private static void writePosting(FileChannel fc, PostingList posting)
			throws IOException {
		/*
		 * TODO: Your code here
		 *	 
		 */
		//Update the file position of the posting
		Pair<Long,Integer> pair = new Pair<Long,Integer>(fc.position(),posting.getList().size());
		
		postingDict.put(posting.getTermId(),pair );
		index.writePosting(fc, posting);
	}
	

	 /**
     * Pop next element if there is one, otherwise return null
     * @param iter an iterator that contains integers
     * @return next element or null
     */
    private static Integer popNextOrNull(Iterator<Integer> iter) {
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }
	
    
   
	
	/**
	 * Main method to start the indexing process.
	 * @param method		:Indexing method. "Basic" by default, but extra credit will be given for those
	 * 			who can implement variable byte (VB) or Gamma index compression algorithm
	 * @param dataDirname	:relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname	:relative path to the output directory to store index. You must not assume
	 * 			that this directory exist. If it does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException 
	{
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}
		
		   
		/* Get output directory*/
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}
		
		/*	TODO: delete all the files/sub folder under outdir
		 * 
		 */
		for(File file: outdir.listFiles()) 
		    if (!file.isDirectory()) 
		        file.delete();
		
		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}
		
		
		
		
		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();
		
		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());
			//System.out.println("Processing block "+block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();
			
			/* For each file */
			
			//Mapping each term to its posting list for each block using its term id
			Map<Integer,PostingList> TermPost = new TreeMap<Integer,PostingList>();
			
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				
				 // use pre-increment to ensure docID > 0
                int docId = ++docIdCounter;
                docDict.put(fileName, docId);
                //add string header
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				PostingList term;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						/*
						 * TODO: Your code here
						 *       For each term, build up a list of
						 *       documents in which the term occurs
						 */
						
						//Use get to check whether termDict contains this id or not
						Integer id=termDict.get(token);
						if(id==null)
						{
							id=++wordIdCounter;
							termDict.put(token,id);
							
							//Create new PostingList and stored in TermPost(TreeMap) for every new term encountered
							term = new PostingList(id);
							term.getList().add(docId);
							TermPost.put(id, term);
						}
						else//if the term already exist in termDict(not a new term)
						{
							//The term exists in the map
							if(TermPost.containsKey(id))
							{
								//If the term is found in the same document, it will be discarded
								if(!TermPost.get(id).getList().contains(docId))
									TermPost.get(id).getList().add(docId);
							}
							/*The term doesn't exist in the map 
							i.e. when we run indexer for the next block, all of the items in TermPost will be discarded*/
							else
							{
								//Create a new PostingList for the term id of new block
								term = new PostingList(id);
								term.getList().add(docId);
								TermPost.put(id, term);
							}
						}
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}
			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			/*
			 * TODO: Your code here
			 *       Write all posting lists for all terms to file (bfc) 
			 */
			/*since keySet() of TreeMap is already sorted
			 *, we can call writePosting function right away */
			for(Integer i:TermPost.keySet())
				writePosting(bfc.getChannel(),TermPost.get(i));
			
			//Discard all item in the map
			TermPost.clear();
			bfc.close();
		}

		/* Required: output total number of files. */
		//System.out.println("Total Files Indexed: "+totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			
			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			 
			/*
			 * TODO: Your code here
			 *       Combine blocks bf1 and bf2 into our combined file, mf
			 *       You will want to consider in what order to merge
			 *       the two blocks (based on term ID, perhaps?).
			 *       
			 */
			//Create FileChannel
			FileChannel f1 = bf1.getChannel();
			FileChannel f2 = bf2.getChannel();
			FileChannel comb = mf.getChannel();
			
			//Get PostingList from each block
			PostingList post1 = index.readPosting(f1);
			PostingList post2 = index.readPosting(f2);
			
			int Id1,Id2,Listsize1,Listsize2;

			//Stop when one of them reaches end of file
			while(post1!=null&&post2!=null)
			{
				//Get Term id and document frequency from each PostingList
				Id1=post1.getTermId();
				Id2=post2.getTermId();
				Listsize1 = post1.getList().size();
				Listsize2 = post2.getList().size();
				
				//Merge PostingList if id1 and id2 are the same term
				if(Id1==Id2)
				{
					
					PostingList tempPost = new PostingList(Id1);
					int i=0,j=0,DocNum1,DocNum2;
					
					//Stop when one of them reaches the end of PostingList
					while(i<Listsize1 && j<Listsize2)
					{
						//Get Document id of post1 and post2
						DocNum1 = post1.getList().get(i);
						DocNum2 = post2.getList().get(j);
						
						//Sort the document id
						if(DocNum1<DocNum2)
						{
							tempPost.getList().add(DocNum1);
							i++;
						}
						else if(DocNum1>DocNum2)
						{
							tempPost.getList().add(DocNum2);
							j++;
						}
						else//If both of them are equal
						{
							tempPost.getList().add(DocNum1);
							i++;
							j++;
						}
					}
					while(i<Listsize1)//Leftover from post1 in case j reaches the end of PostingList
					{
						tempPost.getList().add(post1.getList().get(i));
						i++;
					}
					while(j<Listsize2)//Leftover from post2 in case i reaches the end of PostingList
					{
						tempPost.getList().add(post2.getList().get(j));
						j++;
					}
					writePosting(comb,tempPost);
					//Read new PostingList
					post1 = index.readPosting(f1);
					post2 = index.readPosting(f2);
				}
				//Simply write down the one whose id is lesser
				else if(Id1 < Id2)
				{
					writePosting(comb,post1);
					post1 = index.readPosting(f1);
				}
				else
				{
					writePosting(comb,post2);
					post2 = index.readPosting(f2);
				}
			}
			while(post1!=null)//Leftover from f1 in case f2 reaches the end of file
			{
				writePosting(comb,post1);
				post1 = index.readPosting(f1);
			}
			while(post2!=null)//Leftover from f2 in case f1 reaches the end of file
			{
				writePosting(comb,post2);
				post2 = index.readPosting(f2);
			}
						
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
		
		return totalFileCount;
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err
					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];
		

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
