/*  Member
 *  
 *  1. Thanapon Jarukasetphon  		5888057 	Section: 1
 *  2. Papatsapong Jiraroj-ungkun 	5888060		Section: 1
 *  3. Chatchanin Yimudom  			5888234		Section: 1
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;



public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES*2);//For reading TermID and Document frequency
		try {
			fc.read(buff);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		buff.flip();
		if(buff.hasRemaining())//Check if buff is not null
		{
			int TermID = buff.getInt();
			int DocFreq = buff.getInt();
			ArrayList<Integer> post = new ArrayList<Integer>(DocFreq);
			buff = ByteBuffer.allocate(Integer.BYTES*DocFreq); 
			try {
				fc.read(buff);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			buff.flip();
			//Create posting for TermID
			for(int i=0;i<DocFreq;i++)
			{
				post.add(buff.getInt());
			}
			PostingList p = new PostingList(TermID,post);
			return p;
		}
		return null;//return null when buff is has no remaining
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		int size = p.getList().size();
		int ByteNum = size+2;//+2 from TermID and Number of Document
		ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES*ByteNum);
		buff.putInt(p.getTermId());
		buff.putInt(size);
		// Put document id in buff
		for(int DocID:p.getList())
		{
			buff.putInt(DocID);
		}
		buff.flip();
		try {
			fc.write(buff);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

