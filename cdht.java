import java.io.*;
import java.net.*;
import java.util.*;

public class cdht {

	static int group[] = new int[3];

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out
					.println("Please try the correct input format or I will quit ;P");
			System.exit(-1);
		}
		cdht p = new cdht(args[0], args[1], args[2]);
	}

	// Constructor
	public cdht(String peer, String s1, String s2) throws Exception {
		// first of all, initialise the peer with its ID
		// and tell who are its successors
		group[0] = Integer.parseInt(peer);
		group[1] = Integer.parseInt(s1);
		group[2] = Integer.parseInt(s2);

		// the port number should be greater than 1024
		int port1 = group[0] + 50000;
		int port2 = group[1] + 50000;
		int port3 = group[2] + 50000;
		ping(port1, port2, port3);

	}

	public static void ping(int port1, int port2, int port3) throws IOException {

		// Acts as server, receiving msg from predecessors
		ServerSide s1;
		// start multi-thread
		s1 = new ServerSide(port1);
		Thread t3 = new Thread(s1);
		t3.start();

		// A thread used as client
		// Send message to its successor 1
		ClientSide c1;
		c1 = new ClientSide(port2, group[0], group[1], 0);
		Thread t1 = new Thread(c1);
		t1.start();

		// Send message to its successor 2
		ClientSide c2;
		c2 = new ClientSide(port3, group[0], group[2], 1);
		Thread t2 = new Thread(c2);
		t2.start();

		// Receiving standard input
		InputServer s2;
		s2 = new InputServer(port1, port2, port3);
		Thread t4 = new Thread(s2);
		t4.start();

		// Receiving tcp request from predecessor
		TcpServer s3;
		s3 = new TcpServer(port1, port2, port3);
		Thread t5 = new Thread(s3);
		t5.start();
	}

}

// Multi-thread: As UDP client
class ClientSide extends Thread {
	public int serverPort;
	public int clientID;
	public int serverID;
	public int flag;

	ClientSide(int serverPort, int clientID, int serverID, int flag) {
		this.serverPort = serverPort;
		this.clientID = clientID;
		this.serverID = serverID;
		this.flag = flag;
	}

	// Sending PING while UDP
	public void run() {
		try {
			udpClient();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void udpClient() throws Exception {
		while (true) {
			// Sending UDP request to UDP server
			Thread.sleep(5000);
			DatagramSocket socket = new DatagramSocket();
			String sendMsg = String.valueOf(clientID);
			String temp = new String();
			temp = String.valueOf(flag);
			sendMsg = sendMsg + " " + temp;
			//System.out.println(sendMsg);
			InetAddress servAddr = InetAddress.getByName("127.0.0.1");
			DatagramPacket request = new DatagramPacket(sendMsg.getBytes(),
					sendMsg.getBytes().length, servAddr, serverPort);
			//System.out.println(serverPort);
			socket.send(request);
			DatagramPacket receive = new DatagramPacket(new byte[1024], 1024);
			socket.receive(receive);

			// Getting the response
			System.out
					.println("A ping response message was received from peer "
							+ serverID);
			socket.close();


			// Sending TCP request to TCP server

			String smID = String.valueOf(clientID);
			if(flag == 0) {
				String str2 = "000" + " " + "000" + " " + smID + " 3 0000";
				Socket socket2 = new Socket("127.0.0.1", serverPort);
				DataOutputStream out = new DataOutputStream(
				socket2.getOutputStream());
				out.writeBytes(str2);
				socket2.close();
			}
			if(flag == 1) {
				String str2 = "100" + " " + "000" + " " + smID + " 3 0000";
				Socket socket2 = new Socket("127.0.0.1", serverPort);
				DataOutputStream out = new DataOutputStream(
				socket2.getOutputStream());
				out.writeBytes(str2);
				socket2.close();
			}
		}
	}
}

// Multi-thread: As UDP server
class ServerSide extends Thread {
	public int serverPort;

	// Constructor
	ServerSide(int serverPort) {
		this.serverPort = serverPort;
	}

	public void run() {
		// listen to UDP request
		try {
			udpServer();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void udpServer() throws Exception {
		int firstID = 256;
		int preID = 256;
		int flag = 0;
		while (true) {
			// Receiving the message
			DatagramSocket socket = new DatagramSocket(serverPort);
			DatagramPacket request = new DatagramPacket(new byte[1024], 1024);
			socket.receive(request);
			InetAddress clientHost = request.getAddress();
			int serverID = serverPort - 50000;
			int clientPort = request.getPort();

			// Unpack the message
			byte[] reqMsg = request.getData();
			String str = new String(reqMsg);
			//System.out.println("received msg: ");
			//System.out.println(str);
			int i = 0;
			char[] c = str.toCharArray();
			char[] d = new char[3];
			while (c[i] != ' ') {
				d[i] = c[i];
				i++;
			}
			String t = new String();
			if (i == 1) {
				String temp = Character.toString(d[0]);
				t = "00" + temp;
			}
			if (i == 2) {
				String temp = Character.toString(d[0]);
				t = "0" + temp;
				temp = Character.toString(d[1]);
				t = t + temp;
			}
			if (i == 3) {
				String temp = Character.toString(d[0]);
				t = temp;
				temp = Character.toString(d[1]);
				t = t + temp;
				temp = Character.toString(d[2]);
				t = t + temp;
			}
			i++;
			flag = c[i] - '0';
			if (flag == 0)
				preID = Integer.parseInt(t);
			if (flag == 1)
				firstID = Integer.parseInt(t);

			//System.out.println(preID);
			//System.out.println(firstID);

			// Have received a UDP request
			String sp = new String(d);
			System.out.println("A ping request message was received from peer "
					+ sp);

			// Send to UDP server
			byte[] replymsg = String.valueOf(serverID).getBytes();
			DatagramPacket reply = new DatagramPacket(replymsg,
					replymsg.length, clientHost, clientPort);
			socket.send(reply);
			socket.close();

			/*
			// Send to TCP server
			String sfirstID = String.valueOf(firstID);
			String spreID = String.valueOf(preID);
			String smID = String.valueOf(serverID);
			String str2 = sfirstID + " " + spreID + " " + smID + " 3 0000";
			Socket socket2 = new Socket("127.0.0.1", serverPort);
			DataOutputStream out = new DataOutputStream(
					socket2.getOutputStream());
			out.writeBytes(str2);
			socket2.close();
			*/
		}
	}
}

// Multi-thread: as TCP Server, receiving request from standard input
class InputServer extends Thread {
	public int serverPort;
	public int nextPort;
	public int lastPort;

	InputServer(int serverPort, int nextPort, int lastPort) {
		this.serverPort = serverPort;
		this.nextPort = nextPort;
		this.lastPort = lastPort;
	}

	public void run() {
		try {
			input();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void input() throws IOException {
		int serverID = serverPort - 50000;
		int nextID = nextPort - 50000;
		int lastID = lastPort - 50000;
		// int firstID = 256;
		// int preID = 256;
		// char[] filename = new char[12];
		// int[] temp = new int[4];
		while (true) {
			// read from standard input
			BufferedReader in = new BufferedReader(new InputStreamReader(
					System.in));
			int[] digit = new int[4];
			int i = 0;
			int location = 0;
			int f = 0;
			char c = (char) in.read();
			// System.out.println(c);

			// quit msg
			if (c == 'q') {
				String srvID = Integer.toString(serverID);
				// the msg format is the same
				// but has different meaning
				String str = "000 000 " + srvID + " 2 0000";

				//System.out.println(str);

				Socket outSocket = new Socket("127.0.0.1", serverPort);
				DataOutputStream outToNext = new DataOutputStream(
						outSocket.getOutputStream());
				outToNext.writeBytes(str);
				//System.out.println("I have inform the TCP that I will leave");
				outSocket.close();
			}
			// request msg
			else if (c == 'r') {
				while (c != '\n') {
					c = (char) in.read();
					if (c >= '0' && c <= '9') {
						digit[i] = c - '0';
						i++;
					}
				}
				if (i == 1) {
					f = digit[0];
					location = digit[0] % 256;
				} else if (i == 2) {
					f = digit[0] * 10 + digit[1];
					location = (digit[0] * 10 + digit[1]) % 256;
				} else if (i == 3) {
					f = digit[0] * 100 + digit[1] * 10 + digit[2];
					location = (digit[0] * 100 + digit[1] * 10 + digit[2]) % 256;
				} else if (i == 4) {
					f = digit[0] * 1000 + digit[1] * 100 + digit[2] * 10
							+ digit[3];
					location = (digit[0] * 1000 + digit[1] * 100 + digit[2]
							* 10 + digit[3]) % 256;
				}

				Socket outSocket = new Socket("127.0.0.1", nextPort);

				String fn = new String();
				fn = String.valueOf(f);
				String file = Integer.toString(f);
				String ID = String.valueOf(serverID);
				String fileID = Integer.toString(location);
				String str = ID + " ";
				str = str + fileID;
				str = str + " ";
				str = str + ID;
				str = str + " 0 ";
				str = str + fn;
				str = str + "\n";
				// System.out.println(str);
				// send to server
				DataOutputStream outToNext = new DataOutputStream(
						outSocket.getOutputStream());
				outToNext.writeBytes(str);
				System.out.println("File request message for " + fn
						+ " has been sent to my successor.");
				outSocket.close();
			}
		}
	}

}

class TcpServer extends Thread {
	public int thisPort;
	public int nextPort;
	public int lastPort;

	TcpServer(int thisPort, int nextPort, int lastPort) {
		this.thisPort = thisPort;
		this.nextPort = nextPort;
		this.lastPort = lastPort;
	}

	public void run() {
		try {
			tcp();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void tcp() throws Exception {
		ServerSocket serverSocket = new ServerSocket(thisPort);
		while (true) {
			// whether there is a peer wants to leave
			int quit = 0;
			Socket inSocket = serverSocket.accept();
			BufferedReader in = new BufferedReader(new InputStreamReader(
					inSocket.getInputStream()));
			char[] str = new char[20];
			for (int i = 0; i < 20; i++)
				str[i] = (char) in.read();
			// decoding
			int i = 0;
			int j = 0;
			int k = 0;
			int m = 0;
			int type = 0;
			char[] id = new char[3];
			char[] loca = new char[3];
			char[] pid = new char[3];
			char[] fn = new char[4];

			String s1 = new String();
			String s2 = new String();
			String s3 = new String();
			String s4 = new String();

			// getting peerID
			while (str[i] != ' ') {
				id[i] = str[i];
				i++;
			}
			if (i == 1) {
				s1 = new String("00");
				String temp = Character.toString(id[0]);
				s1 = s1 + temp;
			} else if (i == 2) {
				s1 = new String("0");
				String temp = Character.toString(id[0]);
				s1 = s1 + temp;
				temp = Character.toString(id[1]);
				s1 = s1 + temp;
			} else if (i == 3)
				s1 = new String(id);
			i++;

			// getting fileID
			while (str[i] >= '0' && str[i] <= '9') {
				loca[j] = str[i];
				i++;
				j++;
			}
			if (j == 1) {
				s2 = new String("00");
				String temp = Character.toString(loca[0]);
				s2 = s2 + temp;
			} else if (j == 2) {
				s2 = new String("0");
				String temp = Character.toString(loca[0]);
				s2 = s2 + temp;
				temp = Character.toString(loca[1]);
				s2 = s2 + temp;
			} else if (j == 3)
				s2 = new String(loca);
			i++;

			// getting myID
			while (str[i] >= '0' && str[i] <= '9') {
				pid[k] = str[i];
				i++;
				k++;
			}
			if (k == 1) {
				s3 = new String("00");
				String temp = Character.toString(pid[0]);
				s3 = s3 + temp;
			} else if (k == 2) {
				s3 = new String("0");
				String temp = Character.toString(pid[0]);
				s3 = s3 + temp;
				temp = Character.toString(pid[1]);
				s3 = s3 + temp;
			} else if (k == 3)
				s3 = new String(pid);
			i++;

			// getting type
			type = str[i] - '0';
			i = i + 2;

			// getting filename
			while (str[i] >= '0' && str[i] <= '9') {
				fn[m] = str[i];
				i++;
				m++;
			}
			s4 = new String(fn);

			// System.out.println(fn);
			// System.out.println(s2);
			// System.out.println(s3);

			int peerID = Integer.parseInt(s1);
			int fileID = Integer.parseInt(s2);
			int preID = Integer.parseInt(s3);

			int predID = 0;
			int firstID = 0;

			// DatagramPacket request = new DatagramPacket(new byte[1024],
			// 1024);
			// int preID = request.getPort() * (-1);

			int nextID = nextPort - 50000;
			int myID = thisPort - 50000;
			int lastID = lastPort - 5000;

			if (type == 2) {
				quit = 1;
				//System.out.println("type2");
			}

			if (type == 3 && quit == 1) {
				//System.out.println(s3);
				if(s1 == "000") {
					char[] ppID = new char[3];
					ppID = s3.toCharArray();
					predID = ppID[0] * 100 + ppID[1] * 10 + ppID[1];
					//System.out.println(predID);
				}
				if(s1 == "100") {
					char[] ppID = new char[3];
					ppID = s3.toCharArray();
					firstID = ppID[0] * 100 + ppID[1] * 10 + ppID[1];
					//System.out.println(firstID);
				}
			}

			else if (type == 4) {
				DatagramSocket socket = new DatagramSocket();
				DatagramPacket receive = new DatagramPacket(new byte[1024],
						1024);
				socket.receive(receive);
				byte[] rcvMsg = receive.getData();
				String rMsg = new String(rcvMsg);

				//System.out.println("this is from TCP quit");
				//System.out.println(rMsg);

				InetAddress addr = InetAddress.getByName("127.0.0.1");

				DatagramPacket request = new DatagramPacket(rMsg.getBytes(),
						rMsg.getBytes().length, addr, thisPort);

				socket.send(request);
				socket.close();
			}

			else if (type == 1) {
				String s = new String();
				s = String.valueOf(pid);
				System.out.println("Received a response message from peer " + s
						+ ", which has the file " + s4 + ".");
			}
			// if(myID == peerID) {
			// System.out.println("Received a response message from peer " + s3
			// + ", which has file " + s2 + ".");
			// }

			// System.out.println(preID);
			// System.out.println(nextID);
			// System.out.println(myID);

			// find out whether the file is in my space
			else if (type == 0) {
				if (myID == fileID) {
					System.out.println("File " + s4 + " is here.");
					// send to original peer
					sendBack(peerID, fileID, myID, s4);
				}
				if (myID < fileID) {
					if (myID < nextID) {
						System.out.println("File " + s4
								+ " is not stored here.");
						sendNext(peerID, fileID, myID, s4);
					}
					if (myID > nextID) {
						System.out.println("File " + s4 + " is here.");
						sendBack(peerID, fileID, myID, s4);
					}
				}
				if (myID > fileID) {
					if (fileID > preID) {
						System.out.println("File " + s4 + " is here.");
						sendBack(peerID, fileID, myID, s4);
					}
					if (fileID < preID) {
						System.out.println("File " + s4
								+ " is not stored here.");
						sendNext(peerID, fileID, myID, s4);
					}
				}
			}
			inSocket.close();
		}
	}

	public void sendPre(int firstID, int preID, int myID, int nextID, int lastID)
			throws Exception {
		// send to the first predecessor
		Socket outSocket1 = new Socket("127.0.0.1", firstID + 50000);
		DataOutputStream outToNext1 = new DataOutputStream(
				outSocket1.getOutputStream());
		String str = String.valueOf(preID);
		str = str + " ";
		String temp = String.valueOf(nextID);
		str = str + temp + " ";
		temp = String.valueOf(myID);
		str = str + " 4 0000";
		outToNext1.writeBytes(str);
		outSocket1.close();

		// send to the second predecessor
		Socket outSocket2 = new Socket("127.0.0.1", preID + 50000);
		DataOutputStream outToNext2 = new DataOutputStream(
				outSocket2.getOutputStream());
		String str2 = String.valueOf(nextID);
		str2 = str2 + " ";
		String temp2 = String.valueOf(lastID);
		str2 = str2 + temp2 + " ";
		temp2 = String.valueOf(myID);
		str2 = str2 + " 4 0000";
		outToNext2.writeBytes(str2);
		outSocket2.close();
	}

	public void sendNext(int peerID, int fileID, int myID, String fn)
			throws Exception {
		Socket outSocket = new Socket("127.0.0.1", nextPort);
		DataOutputStream outToNext = new DataOutputStream(
				outSocket.getOutputStream());
		String str = Integer.toString(peerID);
		str = str + " ";
		String str1 = Integer.toString(fileID);
		str = str + str1 + " ";
		String str2 = Integer.toString(myID);
		str = str + str2;
		str = str + " 0 ";
		str = str + fn;
		outToNext.writeBytes(str);
		System.out.println("File request has been forwarded to my successor.");
		outSocket.close();
	}

	public void sendBack(int peerID, int fileID, int myID, String fn)
			throws Exception {
		int peerPort = peerID + 50000;
		Socket outSocket = new Socket("127.0.0.1", peerPort);
		DataOutputStream outToNext = new DataOutputStream(
				outSocket.getOutputStream());
		String str = Integer.toString(peerID);
		str = str + " ";
		String str1 = Integer.toString(fileID);
		str = str + str1 + " ";
		String str2 = Integer.toString(myID);
		str = str + str2;
		str = str + " 1 ";
		str = str + fn;
		String pid = Integer.toString(peerID);
		outToNext.writeBytes(str);
		System.out.println("A response message, destined for peer " + pid
				+ ", has been sent.");
		outSocket.close();
	}
}