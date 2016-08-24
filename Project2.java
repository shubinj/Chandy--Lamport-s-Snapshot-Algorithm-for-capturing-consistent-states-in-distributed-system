import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

public class Project2 {
	int [][] ntwTopo = null;
	int nodesNum = 0;
	int neighbour = 0;
	int nodeId;
	public static int nId;
	Thread[] insertThread;
	Thread[] getThread;
	public static msgPacket appMsg;
	public static msgPacket markerMsg;
	public static msgPacket localSnap;
	public static Integer msgSent = 0;
	public static int[][] tmpMst;
	public static int sendApp = 1;
	int[][] tmpMstTopo ;
	Semaphore rLock= new Semaphore(1);
	Semaphore wLock = new Semaphore(1);
	ArrayList<Integer> nbrList= new ArrayList<Integer>();
	public static ArrayList<Integer> nList = new ArrayList<Integer>();	
	public static ArrayBlockingQueue<Socket> rcvQ = new ArrayBlockingQueue<Socket>(50);
	int isActive= 0;
	int minPerActive= 0;
	int maxPerActive= 0;
	int minSendDelay= 0;
	int snapShotDelay= 0;
	int maxNumber= 0;
	int currMsgCount;	
	public static int mNo;
	public static int cNo;
	public static int isAt;
	public static int minPerA;
	public static int maxPerA;
	public static int minSendD;
	public static int snapShotD;
	public static int neighb;
	public static int snapShotMode;
	public static int markerRecieved;
	public static int totalNodes;
	public static int markerSent;
	public static int isSnapShot = 0;
	public static int allFinished = -2;
	public static File file;
	public static int stop = 0;
	public static int childCount = 0;
	File file1;
	mst obj;
	int[] vectorTime;
	public static Object lockForSendMessage = new Object();
	public static Object lockForSend = new Object();
	Hashtable<Integer,String> nodeHash = new Hashtable<Integer,String>();
	Hashtable<Integer,Socket> outChannel = new Hashtable<Integer,Socket>();
	public static Hashtable<Integer,ObjectOutputStream> ooS = new Hashtable<Integer,ObjectOutputStream>();
	public static Hashtable<Integer,msgPacket> markerHash = new Hashtable<Integer,msgPacket>();
	public static Hashtable<Integer,msgPacket> sSHash = new Hashtable<Integer,msgPacket>();
	public static criticalSection cObj = null;
	public static ArrayBlockingQueue<criticalSection> cObjHash;
	ServerSocket serverSock;	
	ArrayList<Integer> topoMst = new ArrayList<Integer>();;
	public static ArrayList<Integer> mstN = new ArrayList<Integer>();
	
	
	public Project2(String nodeId, String fileName,mst obj) throws IOException,InterruptedException{	
		File fName = new File(fileName);
		FileReader configFile = new FileReader(fileName);
		String tmpF = fName.getName();
		String tmpN = tmpF.split("\\.(?=[^\\.]+$)")[0];
		this.file1 = new File(tmpN+"-"+nodeId+".out");
		int i = 0;
		this.obj = obj;
		this.nodeId = Integer.parseInt(nodeId);
	    this.ntwTopo = obj.topoGraph;
	    this.nodesNum = obj.totalNodes;
	    this.nodeHash = obj.nHash;    	
	    this.minPerActive = this.obj.minPerActive;
	    this.maxPerActive = this.obj.maxPerActive;
	    this.minSendDelay = this.obj.minSendDelay;
	    this.snapShotDelay = this.obj.snapshotDelay;
	    this.maxNumber = this.obj.maxNumber;
	    this.vectorTime = new int[this.nodesNum];
		String[] tmpNode = this.nodeHash.get(this.nodeId).split(":");
     		this.serverSock = new ServerSocket(Integer.valueOf(tmpNode[1]));
	    for(i=0;i<this.nodesNum; i++){
	    	this.vectorTime[i] = 0;
	    	if(this.ntwTopo[this.nodeId][i] == 1){
	    		this.nbrList.add(i);
	    		this.neighbour = this.neighbour + 1;	    		
	    	}	    		                                          
	    }
	
	    this.tmpMstTopo = new int[2][this.nodesNum];
	    for(i=0;i<this.nodesNum;i++){
	    	this.topoMst.add(obj.topoMst[1][i]);
		this.tmpMstTopo[0][i] = obj.topoMst[0][i];
		this.tmpMstTopo[1][i] = obj.topoMst[1][i];
	    }
	   }
	
	void validateSnapShots() throws IOException{
		//Give output to file.
			int i,k;
			int totalSnt = 0;
			int totalRcv = 0;		
			int isActive = 0;
			
			for(i=0;i<(Project2.nList.size());i++){
				if (i == 0){
					totalSnt = totalSnt + Project2.sSHash.get(i).sentM;
					totalRcv = totalRcv + Project2.sSHash.get(i).recvM;
					if (Project2.sSHash.get(i).isActive == 1)
						isActive = 1;
				}
				totalSnt = totalSnt + Project2.sSHash.get(Project2.nList.get(i)).sentM;
				totalRcv = totalRcv + Project2.sSHash.get(Project2.nList.get(i)).recvM;
				if (Project2.sSHash.get(Project2.nList.get(i)).isActive == 1)
					isActive = 1;
			
            }         

  		if ((isActive == 0) && (totalSnt == totalRcv)){
			Project2.allFinished = 1;
		}
     	else
        	    Project2.allFinished = -2;
	}

	

	 void criticalAction(Integer action, Integer currNode, Integer nodeId,msgPacket tmpMsg) throws IOException, InterruptedException, ClassNotFoundException
	 {
	 	int initiate = 0;
		if (action == 1)
		{				
				synchronized(lockForSendMessage)
				{
					if (tmpMsg.FINISH == 1)
					{
						System.out.println(nodeId + " node is trying to send FINISH snapshot of "+tmpMsg.nodeId+"to:"+Project2.nList.get(currNode));
						Project2.ooS.get(Project2.nList.get(currNode)).writeObject(tmpMsg);
					}
					else if (tmpMsg.isSnapShot == 1)
					{
						int i = Project2.nList.size();
						tmpMsg.isActive = Project2.isAt;
						System.out.println(nodeId + " node is trying to send snapshot of "+tmpMsg.nodeId+"to:"+Project2.nList.get(currNode));
						Project2.ooS.get(Project2.nList.get(currNode)).writeObject(tmpMsg);
					}
					else if (tmpMsg.ismarker == 0)
					{
						System.out.println(nodeId + " node is trying to send APP to:"+Project2.nList.get(currNode)+"Am I active :"+Project2.isAt);
						Project2.appMsg.vectorTime[nodeId]= Project2.appMsg.vectorTime[nodeId] + 1;
						Project2.appMsg.sentM = Project2.appMsg.sentM + 1;					
						Project2.ooS.get(Project2.nList.get(currNode)).writeObject(Project2.appMsg);
				    }
					else
					{
					System.out.println(nodeId + " node is trying to send marker to:"+Project2.nList.get(currNode));
					Project2.markerSent = Project2.markerSent + 1;
					Project2.ooS.get(Project2.nList.get(currNode)).writeObject(Project2.markerMsg);
					}
				Project2.ooS.get(Project2.nList.get(currNode)).flush();
				Project2.ooS.get(Project2.nList.get(currNode)).reset();
				Project2.msgSent = 1;							
			}
		}
		else {
				synchronized(lockForSendMessage)
				{					
				
				if (tmpMsg.FINISH == 1)
				{
					 System.out.println("Received FINISHED snapshot from "+tmpMsg.nodeId+" at "+nodeId);
					Project2.allFinished = 1;
				}
				else if(tmpMsg.isSnapShot == 1){
					System.out.println("Received snapshot from "+tmpMsg.nodeId+" at "+nodeId);
					if (Project2.nId == 0){
						Project2.sSHash.put(tmpMsg.nodeId, tmpMsg);
					}
					else{
						System.out.println("Received snapshot from "+tmpMsg.nodeId+" at "+nodeId +" to send to:"+Project2.mstN.get(Project2.nId));
						Project2.cObj = new criticalSection(1,Project2.mstN.get(Project2.nId),Project2.nId,tmpMsg);
						Project2.cObjHash.put(Project2.cObj);
						}
					}
				else if(tmpMsg.ismarker == 0){				
					int k;
					//System.out.println("Received APP from "+tmpMsg.nodeId+" at "+nodeId);
					if (Project2.cNo < Project2.mNo){
					     Project2.isAt = 1;
					}
					int[] tmpVector = tmpMsg.vectorTime.clone();
					for(k=0;k<Project2.totalNodes;k++){
						if (Project2.appMsg.vectorTime[k] >= tmpVector[k])
							tmpVector[k] = Project2.appMsg.vectorTime[k];
					}
					System.out.println("Received app msg from "+tmpMsg.nodeId+" at "+nodeId +" Am I active :"+Project2.isAt);
					System.out.println(Project2.nId+": My vector count: "+Project2.appMsg.vectorTime[nodeId]+" rcvd from "+tmpMsg.nodeId+"at "+nodeId);
					Project2.appMsg.vectorTime = tmpVector.clone();
					int tmpN = Project2.appMsg.vectorTime[nodeId];

					Project2.appMsg.vectorTime[nodeId]= tmpN + 1;					
					Project2.appMsg.recvM = Project2.appMsg.recvM + 1;					
					
					}					
				else
				{
					System.out.println("Received marker from "+tmpMsg.nodeId+" at "+nodeId);
					if (Project2.snapShotMode == 0)
					{
						Project2.snapShotMode = 1;
					}				
						Project2.markerHash.put(tmpMsg.nodeId,tmpMsg);
						Project2.markerRecieved = Project2.markerRecieved + 1;
						Project2.sendApp=0;
                    				if (Project2.snapShotMode == 1 && Project2.isSnapShot == 0 && Project2.nId != 0){					
                    					Project2.localSnap = new msgPacket(1,Project2.nId);
                   	 				Project2.localSnap.recvM = Project2.appMsg.recvM;
                    					Project2.localSnap.sentM = Project2.appMsg.sentM;
                    					Project2.localSnap.isActive = Project2.isAt;
                    					Project2.isSnapShot = 1;
                    					printVector(Project2.appMsg.vectorTime);                    				
                    					Project2.localSnap.isSnapShot = 1;
						
                    				}
				}					
			}
		}		
	}
	 
     public void startSnapShotProto() throws IOException{
    	 //Snapshot process
	   	 BufferedWriter output = new BufferedWriter(new FileWriter(Project2.file));
		 cObjHash = new ArrayBlockingQueue<criticalSection>(Project2.neighb);
    	 output.close();
         class snapShotProcess implements Runnable {
             public void run()
             {
                  while(true){
                     try {                    	
                  	 	if((Project2.allFinished == 1) && (Project2.snapShotMode == 0)){
                  	 		msgPacket finished = new msgPacket(1,Project2.nId);
                            finished.FINISH = 1;
                            finished.isSnapShot = 1;
                            int j;
                            if (Project2.nId == 0){
                               for(j=0;j<Project2.neighb;j++){                 	       					
                 	       			criticalAction(1,j,nId,finished);
                 	       		}                               		          
                             }
                             else{                                
                                for(j = 0;j<Project2.totalNodes;j++){
                                	if (Project2.tmpMst[1][j] == Project2.nId){
                                		criticalAction(1,Project2.nList.indexOf(j),Project2.nId,finished);
                                	}
                                }
                             }
                                	Project2.stop=1;
                                	Thread.sleep(3000);
                                	System.out.println(Project2.nId +" FINISHED!");
                    	       		break;
                         }
                        else{
                        		if ((Project2.nId == 0) && (Project2.sSHash.size() == Project2.neighb+1)){
                        		    validateSnapShots();
                        			Project2.snapShotMode = 0;
                        			Project2.sSHash.clear();
                        		}
					
                        		if ((Project2.snapShotMode == 0) && (Project2.nId == 0)){
                        			Thread.sleep(Project2.snapShotD);
                        			synchronized(lockForSend){
                        			if (Project2.allFinished == 1)
                        				Project2.snapShotMode = 0;
                        			else{                  				
                        					Project2.sendApp = 0;
                        					Project2.snapShotMode = 1;
                        					Project2.markerRecieved = 0;
                        					Project2.markerSent = 0;
                        					Project2.localSnap = new msgPacket(1,Project2.nId);
                        					Project2.localSnap.isSnapShot = 1;
                        					Project2.localSnap.recvM = Project2.appMsg.recvM;
                            				Project2.localSnap.sentM = Project2.appMsg.sentM;
                            				Project2.localSnap.isActive = Project2.isAt;
                        		            	printVector(Project2.appMsg.vectorTime);
                        				Project2.sSHash.put(Project2.nId ,Project2.localSnap);
                        			}
							}
						}
                               	       	
            	       		if((Project2.snapShotMode == 1) && (Project2.markerSent < Project2.neighb)){
            	       		synchronized(lockForSend){
            	       			int i;
            	       			msgPacket tmpP = new msgPacket(1,Project2.nId);
            	       			tmpP.ismarker = 1;
            	       			for(i=0;i<Project2.neighb;i++){
            	       				try {
            								Project2.markerSent = Project2.markerSent + 1;
            								criticalAction(1,i,Project2.nId,tmpP);
            						} catch (ClassNotFoundException e) {
            							// TODO Auto-generated catch block
            							e.printStackTrace();
            						} catch (IOException e) {
            							// TODO Auto-generated catch block
            							e.printStackTrace();
            						}            						
            	       			}
            	       			Project2.sendApp = 1;
            	       		}
            	       		}
               	       		if ((Project2.nId != 0) && (Project2.markerRecieved == Project2.neighb) && (Project2.snapShotMode == 1) && (Project2.cObjHash.size() == Project2.childCount)){
               	       				System.out.println("Initiate transfer of local snapShot using MST!");
                                    int totalRcvd = 0;
                               		int totalSent = 0;
                               		int tmpAct = 0;
                            		while(!Project2.cObjHash.isEmpty()){
                            			criticalSection tmpO = Project2.cObjHash.remove();					
                            			int action = tmpO.action;
                            			int currNode = tmpO.currNode;
                            			int nodeId = tmpO.nodeId;
                            			msgPacket tmpM = tmpO.tmpMsg;
                            			if (tmpM.isActive == 1)
                            				tmpAct = 1;
                            			System.out.println(Project2.nId+":Trying to forward snap from:"+tmpM.nodeId);
                            			Project2.localSnap.recvM = Project2.localSnap.recvM +tmpM.recvM;
                            			Project2.localSnap.sentM = Project2.localSnap.sentM + tmpM.sentM;
                            			//criticalAction(action,Project2.nList.indexOf(Project2.mstN.get(Project2.nId)),Project2.nId,tmpM);
                            			Project2.cObj = null;
                            		}                    		
                            	if (tmpAct == 1)
                            		Project2.localSnap.isActive = 1;                            	
            	       			criticalAction(1,Project2.nList.indexOf(Project2.mstN.get(Project2.nId)),Project2.nId,Project2.localSnap);            	       			
            	       			Project2.snapShotMode = 0;
            	       			Project2.markerSent = 0;
            	       			Project2.markerRecieved = 0;
            	       			Project2.isSnapShot = 0;
            	       		}
               	       				
                        }			
                        Thread.yield();                    	
                        
                     } catch (InterruptedException e) {
                          // TODO Auto-generated catch block
                           e.printStackTrace();
                           } catch (ClassNotFoundException e) {
                        	   // TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                    }
              }
         };
         Thread ssProcess = new Thread(new snapShotProcess());
         ssProcess.start();
 }

    public void rcvChannel() throws IOException,InterruptedException
	{		
    	    int i;
	    Project2.nId = this.nodeId;
	    Project2.file = this.file1;
	    Project2.markerMsg = new msgPacket(1,this.nodeId);	    
	    Project2.appMsg = new msgPacket(0,this.nodeId);
	    Project2.appMsg.vectorTime = this.vectorTime.clone();
	    Project2.cNo = this.currMsgCount;
	    Project2.mNo = this.maxNumber;
	    Project2.nList = (ArrayList<Integer>) this.nbrList.clone();
		Project2.minPerA = this.minPerActive;
		Project2.maxPerA = this.maxPerActive;
		Project2.minSendD = this.minSendDelay;
		Project2.snapShotD = this.snapShotDelay;
		Project2.neighb = this.neighbour;
		Project2.totalNodes = this.nodesNum;
		Project2.mstN = (ArrayList<Integer>) this.topoMst.clone();
		Project2.tmpMst = new int[2][this.nodesNum];
		for (i=0;i<Project2.totalNodes;i++){
			Project2.tmpMst[0][i] = this.tmpMstTopo[0][i];
			Project2.tmpMst[1][i] = this.tmpMstTopo[1][i];
		}
		Project2.tmpMst = this.tmpMstTopo;	
	    if (Project2.nId == 0)
		  	  this.isActive = 1;
		    else
		   	 this.isActive = 0;
	    Project2.isAt = this.isActive;
    	for (i=0;i<Project2.totalNodes;i++){
    		if (tmpMst[1][i] == Project2.nId)
    			Project2.childCount = Project2.childCount + 1;
    	}
		class rcvThread implements Runnable{
			//int isActive;		
			Socket sock;
			Integer nodId;
			Integer fromC;
    		public rcvThread(Socket sock,Integer nId,Integer fC) throws IOException{
    			this.sock = sock;
    			this.nodId = nId;
    			this.fromC = fC;
    		}    		

    		public void run()
    		{	  				
				try {
					ObjectInputStream os;
					this.sock.setKeepAlive(true);
					os = new ObjectInputStream(this.sock.getInputStream());						
					while(true)
					{
						if(Project2.stop == 1){
							os.close();
							this.sock.close();
							break;
							}
						msgPacket tmpMsg = (msgPacket)os.readObject();		
						criticalAction(2,0,this.nodId,tmpMsg);	
						Thread.yield();
					}
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						return;
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						return;
					}					
           	}  	
		
		};	    
		class serverThread implements Runnable {
			ServerSocket serverSock;
			Thread[] connArr;
			Integer nodId;
			serverThread(ServerSocket sock,Integer nId,Integer nNo){
				this.serverSock = sock;
				this.connArr = new Thread[nNo];		
				this.nodId = nId;
			}
			public void run()
			{
				try {				
					int i = 0;
		        	while(true)
		            	{
		        		if(Project2.stop == 1){
						this.serverSock.close();
		        			break;
						}
			            //Listens for a connection to be made to this socket and accepts it
					this.connArr[i] = new Thread(new rcvThread(this.serverSock.accept(),this.nodId,i));
					this.connArr[i].start();
             		    		Thread.yield();				
                 			i=i+1;
		            	}
				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		};
		
		Thread insertThread = new Thread(new serverThread(this.serverSock,this.nodeId, this.neighbour));
		insertThread.start();
		Thread.sleep(4000);

		for(i=0;i<this.nodesNum; i++){
		       if(this.ntwTopo[this.nodeId][i] == 1){
		            String[] tmpNode = this.nodeHash.get(i).split(":");
		            Socket clientSock = new Socket(tmpNode[0],Integer.valueOf(tmpNode[1]));
		            //this.outChannel.put(i, clientSock);		           
    				Project2.ooS.put(i,new ObjectOutputStream(clientSock.getOutputStream()));
			}
		}

		//To initiate sending of messages to neighbours.
		sendChannel();
	}
    
    public void sendChannel() throws IOException
	{		
    	
    		// To send messages to neighbours.
    		class letsSend implements Runnable{
    			int nodeId;
    			letsSend(int nodeId){
    				this.nodeId = nodeId;
    			}
    			
    			public void run(){
    				while(true){
    					int i;    					
    					if (Project2.stop == 1)
    						break;
    											
    					int tmpCount = 0;
    					int currNode = 0;    	       					
    					Random currSend = new Random();
    					int tmpNo = currSend.nextInt(Project2.maxPerA - Project2.minPerA + 1) + Project2.minPerA;	
    					if(Project2.cNo> Project2.mNo)
    						Project2.isAt = 0;
    					while(Project2.isAt==1){
    						synchronized(lockForSend){
							//System.out.println(Project2.nId+": Project2.cNo-"+Project2.cNo+",Project2.mNo-"+Project2.mNo+",tmpCount-"+tmpCount+",tmpNo-"+tmpNo);
    							if ((tmpCount < tmpNo) && (Project2.sendApp == 1) ){
    								try {
    										criticalAction(1,currNode,this.nodeId,Project2.appMsg);
    								} catch (ClassNotFoundException e) {
    									// TODO Auto-generated catch block
    									e.printStackTrace();
    								} catch (InterruptedException e) {
    									// TODO Auto-generated catch block
    									e.printStackTrace();
    								} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
    								try {
    									Thread.sleep(Project2.minSendD);
    								} catch (InterruptedException e) {
    									// TODO Auto-generated catch block
    									e.printStackTrace();
    								}
    								Project2.cNo = Project2.cNo + 1;
    								tmpCount = tmpCount + 1;
    								if ((currNode == (Project2.neighb-1)) && (tmpCount < tmpNo))
    									currNode = 0;
    								else{
    									currNode = currNode + 1;
    								}
    							}
    							else if(tmpCount >= tmpNo){
    								Project2.isAt = 0;
							}
    						}
						
    						}
    				}		
    			}
    		};
    		Thread iThread = new Thread(new letsSend(this.nodeId));
    		iThread.start();
    		startSnapShotProto();
    
	}
    
	public static void printVector(int[] tmpVector) throws IOException{
		int i;
		BufferedWriter output = new BufferedWriter(new FileWriter(Project2.file,true));
		
        for(i=0;i<Project2.totalNodes;i++){		
			int j;
			output.write(tmpVector[i] + "\t");			
        }
        output.write("\n");
        output.close();
	}


	public static void main(String args[]) throws IOException,InterruptedException
		{
		    mst obj = new mst(args[1]);
		    Project2 obj1 = new Project2(args[0],args[1],obj);		    		    
		    obj1.rcvChannel();


		}	
}
