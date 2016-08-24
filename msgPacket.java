import java.io.Serializable;


public class msgPacket implements Serializable{
	int ismarker = 0;
	int isSnapShot = 0;
	int nodeId = 0;
	int[] vectorTime = null;
	int sentM = 0;
	int recvM = 0;
	int enRoute = 0;
	int allSent = 0;
	int FINISH = 0;
	int okFINISH = 0;
	int isActive = 0;
    msgPacket(Integer isM, Integer nId){
            //this.vectorTime = tmpTime.clone();
            this.ismarker = isM;
            this.nodeId = nId;
    }

}
           
