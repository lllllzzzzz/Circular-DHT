import java.util.*;
import java.io.*;
import java.net.*;

public class cdht {
    final int UDP_BASE_PORT = 5000;

    final String MSG_REQUEST_RECV = ">";
    final String MSG_RESPONSE_RECV = "<";
    final String MSG_QUIT_RECV = "X";

    int m_port;
    int[] m_successorPeers, m_predecessorPeers;
    InetAddress m_address;
    DatagramSocket m_datagramSocket;
    ServerSocket m_socketPortPeerInit;
    Timer m_timer;

    public cdht(int port, int[] peersSuccessors, int[] peersPredecessors, InetAddress address, ServerSocket socketPortPeerInit, DatagramSocket datagramSocket) throws IOException {
        final int TIMER_UDP_DELAY = 0;
        final int TIMER_TCP_DELAY = 0;
        final int TIMER_UDP_FREQUENCY = 5000; // Call ping timer (UDP) every 5 second(s)
        final int TIMER_TCP_FREQUENCY = 500;  // Call ping timer (TCP) every 500 millisecond(s)

        m_port = port;
        m_successorPeers = peersSuccessors;
        m_predecessorPeers = peersPredecessors;
        m_address = address;
        m_datagramSocket = datagramSocket;
        m_socketPortPeerInit = socketPortPeerInit;
        
        m_timer = new Timer();

        // Set up timers to run threads
        TimerTask timerUdp = new HandleUdpTasks();
        m_timer.schedule(timerUdp, TIMER_UDP_DELAY, TIMER_UDP_FREQUENCY);
        TimerTask timerTcp = new HandleTcpTasks();
        m_timer.schedule(timerTcp, TIMER_TCP_DELAY, TIMER_TCP_FREQUENCY);
    }

    /*
     * This TimerTask run method handles file requests (TCP) and responds to peers leaving the network via the quit command.
     * This method is called by a timer set to trigger once every 500 milliseconds.
     */
    class HandleTcpTasks extends TimerTask {
        final int NUM_COMPONENTS_MSG_QUIT_RECV = 5;
        
        final String MSG_SENT_RESPONSE_1 = "A response message, destined for peer ";
        final String MSG_SENT_RESPONSE_2 = ", has been sent.";
        final String MSG_FILE_FOUND_RESPONSE_1 = "Received a response message from peer ";
        final String MSG_FILE_FOUND_RESPONSE_2 = " which has the file ";
        final String MSG_DEPART_1 = "Peer ";
        final String MSG_DEPART_2 = " will depart the network.";
        final String MSG_FIRST_SUCCESSOR = "My first successor is now peer ";
        final String MSG_SECOND_SUCCESSOR = "My second successor is now peer ";
        final String MSG_FILE_NOT_FOUND_1 = "File ";
        final String MSG_FILE_NOT_FOUND_2 = " is not stored here.";
        final String MSG_FILE_FOUND_1 = "File ";
        final String MSG_FILE_FOUND_2 = " is here.";
        final String MSG_FORWARD_FILE_REQUEST = "File request message has been forwarded to my successor.";
        final String MSG_REQUEST = ">";
        final String MSG_RESPONSE = "<";

        @Override
        public void run() {
            try {
                Socket connectionSocket = m_socketPortPeerInit.accept();

                BufferedReader userInputReader = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
                String userInput = userInputReader.readLine();

                // Process user input
                String[] userInputCommands = userInput.split(" ");

                // Is this message a request or a response?
                if (userInputCommands[0].equals(MSG_REQUEST_RECV)) {
                    int numFile = Integer.valueOf(userInputCommands[2]);

                    // Calculate which peer is closer to file
                    int diffSuccessorPeer = Math.abs((m_successorPeers[0] - UDP_BASE_PORT) - numFile);
                    int diffThisPeer = Math.abs(numFile - (m_port - UDP_BASE_PORT));

                    // This peer is the closest
                    if ((diffThisPeer == 0) || (diffThisPeer < diffSuccessorPeer)) {
                        System.out.println(MSG_FILE_FOUND_1 + userInputCommands[2] + MSG_FILE_FOUND_2);
                        System.out.println(MSG_SENT_RESPONSE_1 + (Integer.parseInt(userInputCommands[1]) - UDP_BASE_PORT) + MSG_SENT_RESPONSE_2);
                        sendMessage(MSG_RESPONSE + " " + userInputCommands[1] + " " + userInputCommands[2] + " " + m_port + " " + userInputCommands[1]);
                    // Otherwise, the successor peer is the closest
                    } else {
                        System.out.println(MSG_FILE_NOT_FOUND_1 + userInputCommands[2] + MSG_FILE_NOT_FOUND_2);
                        System.out.println(MSG_FORWARD_FILE_REQUEST);
                        sendMessage(MSG_REQUEST + " " + userInputCommands[1] + " " + userInputCommands[2] + " " + m_port + " " + m_successorPeers[0]);
                    }
                }

                // This peer has the file, process response message to send to requesting peer
                if (userInputCommands[0].equals(MSG_RESPONSE_RECV)) {
                    System.out.println(MSG_FILE_FOUND_RESPONSE_1 + (Integer.parseInt(userInputCommands[3]) - UDP_BASE_PORT) + MSG_FILE_FOUND_RESPONSE_2 + userInputCommands[2] + ".");
                }

                // Peer leaving network?
                if (userInputCommands[0].equals(MSG_QUIT_RECV)) {
                    int[] input = new int[NUM_COMPONENTS_MSG_QUIT_RECV];
                    
                    // Break up message into components
                    for (int i = 0; i < NUM_COMPONENTS_MSG_QUIT_RECV; i++) {
                        input[i] = Integer.valueOf(userInputCommands[i + 1]);
                    }

                    // Update successor node mappings
                    if (m_port == input[(input[0] > input[1]) ? 0 : 1]) {
                        m_successorPeers[(input[4] == m_successorPeers[0]) ? 0 : 1] = input[(input[2] > input[3]) ? 2 : 3];
                    }

                    if (m_port == input[(input[0] < input[1]) ? 0 : 1]) {
                        m_successorPeers[((input[4] == m_successorPeers[0]) ? 0 : 1)] = input[(input[2] < input[3]) ? 2 : 3];
                    }
                    
                    sortSuccessorPeers(m_successorPeers);

                    // Print new successor messages to user
                    System.out.println(MSG_DEPART_1 + (input[4] - UDP_BASE_PORT) + MSG_DEPART_2);
                    System.out.println(MSG_FIRST_SUCCESSOR + (m_successorPeers[0] - UDP_BASE_PORT) + ".");
                    System.out.println(MSG_SECOND_SUCCESSOR + (m_successorPeers[1] - UDP_BASE_PORT) + ".");
                }
                
                connectionSocket.close();
            } catch (IOException e) {
            }
        }
    }

    /*
     * This TimerTask run method calls PingNodeLoop() which handles ping requests/responses on the network (UDP).
     * This method is called by a timer set to trigger once every 5 seconds i.e. pings are handled every 5 seconds.
     */
    class HandleUdpTasks extends TimerTask {
        // Ping peers 144 times. This method is called once every 5 seconds.
        // 144 * 5 = 720 seconds = 12 minutes, meeting 12-minute time limit.
        final int NUM_TIMES_TO_PING = 144;

        final String ERROR_PING_PEERS = "Pinging failed!";
        final String TIME_LIMIT_EXCEEDED = "12 minutes have elapsed, will stop pinging now.";
        
        // Counter to limit number of pings
        int numTimesToPing = NUM_TIMES_TO_PING;

        @Override
        public void run() {
            // Ping limit not yet reached, continue pinging
            if (numTimesToPing-- > 0) {
                try {
                    PingNodeLoop(m_address, m_port, m_successorPeers);
                } catch (Exception e) {
                    System.out.println(ERROR_PING_PEERS);
                }
            // Ping limit reached, let user know 12 minutes have elapsed and exit
            } else {
                System.out.println(TIME_LIMIT_EXCEEDED);
                System.exit(0);
            }
        }
    }

    /*
     * This method is the loop that pings successor peers and responds to pings from predecessor peers.
     * This method is called from a timer thread that called once every 10 seconds (by default).
     */
    public void PingNodeLoop(InetAddress address, int portPeerInit, int[] peersSuccessors) throws Exception {
        final int SIZE_SEND_DATA_BUFFER = 128;
        final int SIZE_RECV_DATA_BUFFER = 128;
        final int SIZE_RESPONSE_DATA_BUFFER = 128;
        final int NUM_PINGS_TO_SEND = 2;

        final String MSG_PING = "!";
        final String MSG_RESPONSE = "<";
        final String MSG_PING_REQUEST_RECEIVED = "A ping request message was received from peer ";
        final String MSG_PING_RESPONSE_RECEIVED = "A ping response message was received from peer ";

        // Send ping to each of two successor peers
        for (int i = 0; i < NUM_PINGS_TO_SEND; i++) {
            byte[] sendData = new byte[SIZE_SEND_DATA_BUFFER];
            sendData = MSG_PING.getBytes();

            DatagramPacket recvDataPacket = new DatagramPacket(sendData, sendData.length, address, peersSuccessors[i]);
            m_datagramSocket.send(recvDataPacket);
        }

        byte[] receiveData = new byte[SIZE_RECV_DATA_BUFFER];

        // Initialise response to compare messages
        byte[] cmpMsg = new byte[SIZE_RESPONSE_DATA_BUFFER];
        cmpMsg = MSG_PING.getBytes();
        Byte cmpMsgByte1 = new Byte(cmpMsg[0]);

        // Initialise response string to send to peer that is source of ping
        byte[] respondMsg = new byte[SIZE_RESPONSE_DATA_BUFFER];
        respondMsg = MSG_RESPONSE.getBytes();
        Byte respondMsgByte1 = new Byte(respondMsg[0]);

        for (int i = 0; i < NUM_PINGS_TO_SEND; i++) {
            DatagramPacket recvDataPacket = new DatagramPacket(receiveData, receiveData.length);

            try {
                m_datagramSocket.receive(recvDataPacket);
            } catch (SocketTimeoutException e) {
                continue;
            }

            byte[] recvDataPacketBytes = recvDataPacket.getData();
            Byte recvDataPacketByte1 = new Byte(recvDataPacketBytes[0]);
            int recvDataPort = recvDataPacket.getPort();

            if (recvDataPacketByte1.equals(respondMsgByte1)) {
                // Print "ping response received" message to user
                System.out.println(MSG_PING_RESPONSE_RECEIVED + (recvDataPort - UDP_BASE_PORT) + ".");
            } else if (recvDataPacketByte1.equals(cmpMsgByte1)) {
                // Print "ping request received" message to user
                System.out.println(MSG_PING_REQUEST_RECEIVED + (recvDataPort - UDP_BASE_PORT) + ".");

                // Add to predecessors array
                if ((m_predecessorPeers[0] != recvDataPort) && (m_predecessorPeers[1] != recvDataPort)) {
                    m_predecessorPeers[i] = recvDataPort;
                    sortSuccessorPeers(m_predecessorPeers);
                }

                // Create new packet for response message
                DatagramPacket respondDataPacket = new DatagramPacket(respondMsg, respondMsg.length, address, recvDataPort);
                m_datagramSocket.send(respondDataPacket);
            }
        }
    }

    /*
     * Send message to peers on the network.
     */
    public void sendMessage(String message) {
        final String ERROR_FORWARD_MESSAGE = "Failed to forward message! Is peer connected to the network?";

        try {
            // Create new socket for sending message
            String[] messageComponents = message.split(" ");
            int newSender = Integer.valueOf(messageComponents[4]);
            Socket sendMsgSocket = new Socket(m_address, newSender);

            // Attach data output stream to socket
            DataOutputStream outputDataStream = new DataOutputStream(sendMsgSocket.getOutputStream());

            // Send data to nodes on network
            String outputString = messageComponents[0] + " " + messageComponents[1] + " " + messageComponents[2] + " " + messageComponents[3] + " " + messageComponents[4] + "\n";
            outputDataStream.writeBytes(outputString);
            
            sendMsgSocket.close();
        } catch (IOException e) {
            System.out.println(ERROR_FORWARD_MESSAGE);
        }
    }

    /*
     * Return true if string is an integer, false otherwise.
     */
    public static boolean isInteger(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException nfe) {
        }

        return false;
    }

    /*
     * Return hash of filename.
     */
    public static int convertToHash(String hashTarget) {
        final int HASH_MODULO = 256;
        
        return (Integer.parseInt(hashTarget) + 1) % HASH_MODULO;
    }
    
    /*
     * Sort successor peers.
     */
    public static void sortSuccessorPeers(int[] arr) {
        final int NUM_SUCCESSOR_PEERS = 2;
        
        if (arr.length == NUM_SUCCESSOR_PEERS) {
            int temp;

            if (arr[0] > arr[1]) {
                temp = arr[1];
                arr[1] = arr[0];
                arr[0] = temp;
            }
        }
    }

    public static void main(String args[]) throws Exception {
        final int NUM_GOOD_ARGS = 3;
        final int NUM_PEERS_TO_PING = 2;
        final int NUM_PEERS_PREDECESSORS = 2;
        final int UDP_BASE_PORT = 5000;
        final int INDEX_INIT_PEER_ID = 0;
        final int INDEX_SUCCESSIVE_1_PEER_ID = 1;
        final int INDEX_SUCCESSIVE_2_PEER_ID = 2;
        final int TIMEOUT_WELCOME_SOCKET = 500;
        final int TIMEOUT_PEER_INIT_SOCKET = 2000;
        final int FREQUENCY_PING = 10000;
        final int SIZE_REQUEST_MSG = 2;
        final int LENGTH_FILE_NUM = 4;

        final String ERROR_BAD_ARGS = "Usage: java cdht [init peer id] [successive peer 1 id] [successive peer 2 id]";
        final String MSG_PEER_HELLO_1 = "Hello world! I am peer ";
        final String MSG_PEER_HELLO_2 = " on port ";
        final String MSG_REQUEST = "request";
        final String MSG_REQUEST_SEND = "> ";
        final String MSG_QUIT = "quit";
        final String MSG_QUIT_SEND = "X ";
        final String ERROR_CONNECT_TO_PORT = "Failed to connect to server port!";
        final String ERROR_PINGS_NO_RESPONSE = "Still awaiting ping responses...";
        final String MSG_QUIT_GOODBYE = "I am leaving the network now!";

        if (args.length != NUM_GOOD_ARGS || !isInteger(args[0]) || !isInteger(args[1]) || !isInteger(args[2])) {
            System.out.println(ERROR_BAD_ARGS);
            System.exit(0);
        }

        InetAddress addressLocalhost = InetAddress.getLocalHost();

        // Get ID of peer to initialise
        int peerIdInit = Integer.parseInt(args[INDEX_INIT_PEER_ID]);
        int portPeerInit = UDP_BASE_PORT + peerIdInit;

        // Calculate IDs of successor peers
        int[] peersSuccessors = new int[NUM_PEERS_TO_PING];
        peersSuccessors[0] = UDP_BASE_PORT + Integer.parseInt(args[INDEX_SUCCESSIVE_1_PEER_ID]);
        peersSuccessors[1] = UDP_BASE_PORT + Integer.parseInt(args[INDEX_SUCCESSIVE_2_PEER_ID]);

        ServerSocket socketPortPeerInit = new ServerSocket(portPeerInit);
        socketPortPeerInit.setSoTimeout(TIMEOUT_WELCOME_SOCKET);

        System.out.println(MSG_PEER_HELLO_1 + peerIdInit + MSG_PEER_HELLO_2 + portPeerInit + ".");

        // Set up socket using port of peer to initialise
        DatagramSocket datagramSocket = new DatagramSocket(portPeerInit);
        datagramSocket.setSoTimeout(TIMEOUT_PEER_INIT_SOCKET);
        
        int[] peersPredecessors = new int[NUM_PEERS_PREDECESSORS];
        // Initialise this peer on the network
        new cdht(portPeerInit, peersSuccessors, peersPredecessors, addressLocalhost, socketPortPeerInit, datagramSocket);

        while (true) {
            BufferedReader userInputReader = new BufferedReader(new InputStreamReader(System.in));
            String userInput = userInputReader.readLine();
            String[] userInputCommands = userInput.split(" ");

            String stringPortPeerInit = String.valueOf(portPeerInit);

            if (userInputCommands[0].equals(MSG_REQUEST)) {
                if (userInputCommands.length != SIZE_REQUEST_MSG || !isInteger(userInputCommands[1]) || userInputCommands[1].length() != LENGTH_FILE_NUM) {
                    continue;
                }

                String fileNumHash = String.valueOf(convertToHash(userInputCommands[1]));

                try {
                    // Create new socket to communicate with successor peer
                    Socket socketPredecessorPeer1 = new Socket(addressLocalhost, peersSuccessors[0]);
                    // Attach datastream to socket
                    DataOutputStream outputDataStream = new DataOutputStream(socketPredecessorPeer1.getOutputStream());
                    // Send data to peers on network
                    outputDataStream.writeBytes(MSG_REQUEST_SEND + stringPortPeerInit + " " + fileNumHash + " " + "\n");

                    //socketPredecessorPeer1.close();
                } catch (Exception e) {
                    System.out.println(ERROR_CONNECT_TO_PORT);
                }
            } else if (userInputCommands[0].equals(MSG_QUIT)) {
                try {
                    String STRING_QUIT = MSG_QUIT_SEND + peersPredecessors[0] + " " + peersPredecessors[1] + " " + peersSuccessors[0] + " " + peersSuccessors[1] + " " + stringPortPeerInit + "\n";

                    // Create sockets for predecessor nodes
                    Socket socketPredecessorPeer1 = new Socket(addressLocalhost, peersPredecessors[0]);
                    Socket socketPredecessorPeer2 = new Socket(addressLocalhost, peersPredecessors[1]);

                    // Open output streams to communicate with predecessor nodes
                    DataOutputStream outputStreamPredecessorPeer1 = new DataOutputStream(socketPredecessorPeer1.getOutputStream());
                    DataOutputStream outputStreamPredecessorPeer2 = new DataOutputStream(socketPredecessorPeer2.getOutputStream());

                    // Send quit message to predecessor nodes
                    outputStreamPredecessorPeer1.writeBytes(STRING_QUIT);
                    socketPredecessorPeer1.close();
                    outputStreamPredecessorPeer2.writeBytes(STRING_QUIT);
                    socketPredecessorPeer2.close();

                    // Print quit message to user and leave network
                    System.out.println(MSG_QUIT_GOODBYE);
                    System.exit(0);
                } catch (Exception e) {
                    System.out.println(ERROR_PINGS_NO_RESPONSE);
                }
            }
        }
    }
}
