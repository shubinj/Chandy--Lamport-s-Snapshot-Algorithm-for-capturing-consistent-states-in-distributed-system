
public class criticalSection {
	Integer action;
	Integer currNode;
	Integer nodeId;
	msgPacket tmpMsg;
	criticalSection(Integer action, Integer currNode, Integer nodeId,msgPacket tmpMsg){
		this.action = action;
		this.currNode = currNode;
		this.nodeId = nodeId;
		this.tmpMsg = tmpMsg;
	}
}
