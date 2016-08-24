import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;


public class mst {
	int totalNodes = 0;
	int [][] topoGraph = null;
	int [][] topoMst = null;
	File file = null;
	int minPerActive,maxPerActive,minSendDelay,snapshotDelay,maxNumber;
	BufferedWriter output = null;
	ArrayList<Integer> nodeList = new ArrayList<Integer>();    
    Hashtable<Integer,String> nHash = new Hashtable<Integer,String>();
    
	mst(String fileName){
		try {
			parseConfig(fileName);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int i,j;
		for (i=0;i<this.totalNodes;i++)
		{
			for (j=0;j<this.totalNodes;j++){
				System.out.print(this.topoGraph[i][j]+"\t");			
			}	
			System.out.println("\n");

		}		

		createMst();
		System.out.println("Converge cast MST is:\n");
		for (i=0;i<this.totalNodes;i++)
		{
			System.out.print(this.topoMst[0][i]+"\t");

		}
		System.out.println("\n");
		for (i=0;i<this.totalNodes;i++)
		{
			System.out.print(this.topoMst[1][i]+"\t");
		}		
		
	}
	
	void createMatrix(){
		int i= 0,j=0;
		this.topoGraph = new int[this.totalNodes][this.totalNodes];
		this.topoMst = new int[2][this.totalNodes];
		for (i=0;i<this.totalNodes;i++)
		{
			this.topoMst[0][i]=9999;
			this.topoMst[1][i]=0;
			for (j=0;j<this.totalNodes;j++){
				topoGraph[i][j]=0;			
			}				
		}		
	}


	void createMst(){
		int i,j;
		this.topoMst[0][0]=0;
		this.topoMst[1][0]=0;
		for (i=0;i<this.totalNodes;i++){
			for (j=0;j<this.totalNodes;j++){				
				if (this.topoGraph[i][j] != 0){
					if(this.topoMst[0][j] == 9999){
	                	              this.topoMst[0][j] = this.topoGraph[i][j];
                                	      this.topoMst[1][j] = i;
                               		 }
					else if (this.topoMst[0][j] > this.topoGraph[0][i]+1){
						this.topoMst[0][j] = this.topoGraph[0][i]+1;
						this.topoMst[1][j] = i;
					}
				}
			}
		}
	}
	
	void parseConfig(String fileName) throws IOException{
			File fName = new File(fileName);
			FileReader configFile = new FileReader(fileName);
			BufferedReader bRead = new BufferedReader(configFile);
			String newLine = null;
			int grpHash=1;
			int firstLine=0;
			
			String tmpF = fName.getName();
			String tmpN = tmpF.split("\\.(?=[^\\.]+$)")[0];
		        int count = 0;
			while ((newLine = bRead.readLine()) != null)
			{ 
				if(newLine.isEmpty())
					continue;
				if (newLine.startsWith("#"))
					continue;
				String[] tmpList = newLine.split("\\s+");
				if (firstLine == 0){
					this.minPerActive = Integer.parseInt(tmpList[1]);
					this.maxPerActive = Integer.parseInt(tmpList[2]);
					this.minSendDelay = Integer.parseInt(tmpList[3]);
					this.snapshotDelay = Integer.parseInt(tmpList[4]);
					this.maxNumber = Integer.parseInt(tmpList[5]);
					firstLine = 1;
				}
				if(nodeList.contains(Integer.parseInt(tmpList[0])) && (grpHash == 1)){
				       grpHash = 2;
				       this.createMatrix();
				}

				if((grpHash == 1) && (tmpList.length!=3))
					continue;

				if (grpHash == 1){
					String[] tmpNL = newLine.split("\\s+");
					nHash.put(Integer.parseInt(tmpNL[0]),tmpNL[1]+":"+tmpNL[2]);
					nodeList.add(Integer.parseInt(tmpNL[0]));
					this.totalNodes= this.totalNodes+1;	
				}				
				if (grpHash == 2){					
					newLine = newLine.split("#")[0];
					tmpList = newLine.split("\\s+");
					int i;
					String tmpLine=null;					
					for(i=0;i< tmpList.length;i++)
					{
						if (tmpList[i].startsWith("#"))
							continue;
						int idx= Integer.parseInt(tmpList[i]);
						topoGraph[count][idx]=1;
						topoGraph[idx][count]=1;
					}
					count=count+1;							
				}		

			}
		
	}

	public static void main(String args[]) throws IOException,InterruptedException
	{
	    mst obj = new mst(args[0]);
	}
}

