package com.cs425.membership;

import com.cs425.Messages.FileMessage;
import com.cs425.Messages.MembershipMessage;
import com.cs425.Messages.MembershipMessage.MessageType;
import com.cs425.fileSystem.Coordinator;
import com.cs425.fileSystem.FileServer;
import com.cs425.membership.MembershipList.MemberList;
import com.cs425.membership.MembershipList.MemberListEntry;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Member {

    // Member & introducer info
    private String host;
    private int port;
    private Date timestamp;
    public String introducerHost;
    public int introducerPort;

    // Protocol settings
    private static final long PROTOCOL_TIME = 1500;
    private static final int NUM_MONITORS = 1;

    // Sockets
    private ServerSocket server;
    private DatagramSocket socket;

    // Membership list and owner entry
    public volatile MemberList memberList;
    public MemberListEntry selfEntry;

    // Threading resources
    private Thread mainProtocolThread;
    private Thread TCPListenerThread;
    private FileServer fileServer;

    private AtomicBoolean joined;
    private AtomicBoolean end;
    private AtomicBoolean electionOngoing;

    // Reference to coordinator if this node elected
    private Coordinator coordinator = null;

    // Logger
    public static Logger logger = Logger.getLogger("MemberLogger");

    // Constructor and beginning of functionality
    public Member(String host, int port, String introducerHost, int introducerPort) throws SecurityException, IOException {
        assert(host != null);
        assert(timestamp != null);

        this.host = host;
        this.port = port;

        this.introducerHost = introducerHost;
        this.introducerPort = introducerPort;

        joined = new AtomicBoolean();
        end = new AtomicBoolean();
        electionOngoing = new AtomicBoolean();

        Handler fh = new FileHandler("/srv/mp2_logs/member.log");
        fh.setFormatter(new SimpleFormatter());
        logger.setUseParentHandlers(false);
        logger.addHandler(fh);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    // Process command line inputs
    public void start() throws ClassNotFoundException, InterruptedException {
        logger.info("Member process started");
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("MemberProcess$ ");
                String[] input = stdin.readLine().split(" ");
                String command = input[0];
                System.out.println();
                long startTime = System.currentTimeMillis();
                switch (command) {
                    case "join":
                        joinGroup();
                        break;
                
                    case "leave":
                        leaveGroup(true);
                        break;
                
                    case "list_mem":
                        if (joined.get()) {
                            System.out.println(memberList);
                        } else {
                            System.out.println("Not joined");
                        }
                        break;
                
                    case "list_self":
                        if (joined.get()) {
                            System.out.println(selfEntry);
                        } else {
                            System.out.println("Not joined");
                        }
                        break;
                    case "store":
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(null, null, input[0], null);
                        break;
                    case "put":
                        //TODO: verify all required inputs present and handle alternate case
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(input[1], input[2].replace('/','#'), input[0], 0);
                        break;
                    case "get":
                        //TODO: verify all required inputs present
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(input[2], input[1].replace('/','#'), input[0], 0);
                        break;
                    case "delete":
                        //TODO: verify all required inputs present
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands("", input[1].replace('/','#'), input[0], 0);
                        break;
                    case "ls":
                        //TODO: verify all required inputs present
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands("", input[1].replace('/','#'), input[0], 0);
                        break;
                    case "get-versions":
                        //TODO: verify all required inputs present
                        if (!joined.get()) {
                            System.out.println("Not joined");
                            break;
                        }
                        fileServer.processFileCommands(input[3], input[1].replace('/','#'), input[0], Integer.parseInt(input[2]));
                        break;
                    default:
                    System.out.println("Unrecognized command, type 'join', 'leave', 'list_mem','list_self', ' put localfilename sdfsfilename', " +
                            "' get sdfsfilename localfilename', 'delete sdfsfilename', 'ls sdfsfilename', 'store', 'get-versions sdfsfilename num-versions localfilename'");
                        break;
                }
                logger.info("Time taken to process the command " + command + " is " + (System.currentTimeMillis() - startTime));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println();
        }
    }

    private void joinGroup() throws IOException, ClassNotFoundException {
        // Do nothing if already joined
        if (joined.get()) {
            return;
        }

        logger.info("Member process received join command");

        end.set(false);

        // Initialize incarnation identity
        this.timestamp = new Date();
        this.selfEntry = new MemberListEntry(host, port, timestamp);

        logger.info("New entry created: " + selfEntry);

        // Get member already in group
        MemberListEntry groupProcess = getGroupProcess();

        boolean firstMember = false;

        if (groupProcess != null) {
            // Get member list from group member and add self
            memberList = requestMemberList(groupProcess);
            memberList.addNewOwner(selfEntry);
            logger.info("Retrieved membership list");
        } else {
            // This is the first member of the group
            logger.info("First member of group");
            memberList = new MemberList(selfEntry);

            firstMember = true;
        }

        // Create file server
        fileServer = new FileServer(host, port, memberList);
        logger.info("File server created");

        // Create TCP server socket
        server = new ServerSocket(port);

        // Start server for handling TCP messages
        TCPListenerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member.this.TCPListener();
            }
        });
        TCPListenerThread.start();
        logger.info("TCP Server started");

        if (firstMember) {
            memberList.updateCoordinator(selfEntry);
            coordinator = new Coordinator(memberList, end);
        }

        // Communicate join
        disseminateMessage(new MembershipMessage(MessageType.Join, selfEntry));
        logger.info("Process joined");

        // Start main protocol
        mainProtocolThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Member.this.mainProtocol();
            }
        });
        mainProtocolThread.setDaemon(true);
        mainProtocolThread.start();
        logger.info("Main protocol started");

        joined.set(true);
    }

    // Fetch member details of member already present in group
    private MemberListEntry getGroupProcess() throws IOException, ClassNotFoundException {
        Socket introducer = new Socket(introducerHost, introducerPort);
        logger.info("Connected to " + introducer);
        ObjectOutputStream output = new ObjectOutputStream(introducer.getOutputStream());
        ObjectInputStream input = new ObjectInputStream(introducer.getInputStream());

        // Send self entry to introducer
        output.writeObject(selfEntry);
        output.flush();

        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(selfEntry) + " bytes over TCP to introducer");

        // receive running process
        MemberListEntry runningProcess = (MemberListEntry) input.readObject();

        // Close resources
        input.close();
        output.close();
        introducer.close();

        logger.info("Connection to introducer closed");

        return runningProcess;
    }

    // Fetch membership details from a member already in group
    private MemberList requestMemberList(MemberListEntry groupProcess) throws IOException, ClassNotFoundException {
        Socket client = new Socket(groupProcess.getHostname(), groupProcess.getPort());
        ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
        ObjectInputStream input = new ObjectInputStream(client.getInputStream());


        // Request membership list
        MembershipMessage message = new MembershipMessage(MessageType.MemberListRequest, selfEntry);

        output.writeObject(message);
        output.flush();
        logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for membership list request");

        MemberList retrievedList = (MemberList) input.readObject();

        // Close resources
        input.close();
        output.close();
        client.close();

        return retrievedList;
    }

    private void leaveGroup(boolean sendMessage) throws IOException, InterruptedException {
        // Do nothing if not joined
        if (!joined.get()) {
            return;
        }

        logger.info("Leave command received");

        // Disseminate leave if necessary
        if (sendMessage) {
            disseminateMessage(new MembershipMessage(MessageType.Leave, selfEntry));
            logger.info("Request to leave disseminated");
        }

        // Close resources
        end.set(true);

        mainProtocolThread.join();
        logger.info("Main Protocol stopped");

        server.close();
        TCPListenerThread.join();
        logger.info("TCP server closed");

        memberList = null;
        selfEntry = null;
        coordinator = null;
        
        logger.info("Process left");
        joined.set(false);
    }

    // Uses fire-and-forget paradigm
    public void disseminateMessage(MembershipMessage message) {
        synchronized (memberList) {
            for (MemberListEntry entry: memberList) {
                // Don't send a message to ourself
                if (entry.equals(selfEntry)) {
                    continue;
                }

                try {
                    // Open resources
                    Socket groupMember = new Socket(entry.getHostname(), entry.getPort());
                    ObjectOutputStream output = new ObjectOutputStream(groupMember.getOutputStream());
                    ObjectInputStream input = new ObjectInputStream(groupMember.getInputStream());

                    // Send message
                    output.writeObject(message);
                    output.flush();

                    switch (message.getMessageType()) {
                        case Join:
                            logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case Leave:
                            logger.info("LEAVE: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        case Crash:
                            logger.info("CRASH: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP for dissemination");
                            break;
                        default:
                            assert(false);
                    }

                    // Close resources
                    input.close();
                    output.close();
                    groupMember.close();
                } catch (IOException e) {
                    continue;
                }
            }
        }
    }

    // Uses fire-and-forget paradigm
    public void sendMessageToSuccessor(MembershipMessage message) {
        MemberListEntry successor;

        synchronized (memberList) {
            // Successor should exist
            assert(memberList.hasSuccessor());

            successor = memberList.getSuccessors(1).get(0);
        }

        try {
            // Open resources
            Socket successorSocket = new Socket(successor.getHostname(), successor.getPort());
            ObjectOutputStream output = new ObjectOutputStream(successorSocket.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(successorSocket.getInputStream());

            // Send message
            output.writeObject(message);
            output.flush();

            logger.info("Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP to successor");

            // Close resources
            input.close();
            output.close();
            successorSocket.close();
        } catch (IOException e) {
            return;
        }
    }


    // Thread methods
    private void TCPListener() {
        while (!end.get()) {
            try {
                Socket client = server.accept();
                logger.info("TCP connection established from " + client.toString());

                // Process message in own thread to prevent race condition on membership list
                Thread processMessageThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        Member.this.processTCPMessage(client);
                    }
                });
                processMessageThread.start();

            } catch(SocketException e) {
                break;
            } catch (Exception e) {
                continue;
            }
        }
        
    }

    private void processTCPMessage(Socket client) {
        try {
            // Open resources
            ObjectOutputStream output = new ObjectOutputStream(client.getOutputStream());
            ObjectInputStream input = new ObjectInputStream(client.getInputStream());

            // Recieve message
            Object message = input.readObject();

            // Process based on message type
            if (message instanceof MembershipMessage) {
                processMembershipMessage((MembershipMessage) message, client, input, output);
            } else if (message instanceof FileMessage) {
                processFileMessage((FileMessage) message, client, input, output);
            } else {
                assert false: "Unrecognized message type";
            }

            // Close resources
            input.close();
            output.close();
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Process membership related messages message
    private void processMembershipMessage(MembershipMessage message, Socket client, ObjectInputStream input, ObjectOutputStream output) throws IOException, InterruptedException {
        // Perform appropriate action
        switch(message.getMessageType()) {
            case MemberListRequest:
                synchronized (memberList) {
                    output.writeObject(memberList);
                    output.flush();
                    logger.info("JOIN: Sent " + ObjectSize.sizeInBytes(message) + " bytes over TCP containing membership list");
                }
                break;

            case Join:
                logger.info("Received message for process joining group: " + message.getSubjectEntry());
                synchronized (memberList) {
                    if (memberList.addEntry(message.getSubjectEntry())) {
                        logger.info("Process added to membership list: " + message.getSubjectEntry());
                    }
                }
                break;

            case Leave:
                logger.info("Received message for process leaving group: " + message.getSubjectEntry());
                synchronized (memberList) {
                    if (memberList.removeEntry(message.getSubjectEntry())) {
                        logger.info("Process removed from membership list: " + message.getSubjectEntry());
                    }
                }
                // If coordinator left
                if (memberList.getCoordinator() == null) {
                    if (memberList.hasSuccessor()) {
                        // Start election if needed
                        if(!electionOngoing.get()) {
                            electionOngoing.set(true);
                            sendMessageToSuccessor(new MembershipMessage(MessageType.ElectionId, selfEntry));
                        }
                    } else {
                        // If no successors exist, this process is automatically the new coordinator
                        memberList.updateCoordinator(selfEntry);
                        coordinator = new Coordinator(memberList, end);
                    }
                }
                break;

            case Crash:
                logger.warning("Message received for crashed process: " + message.getSubjectEntry());
                if (selfEntry.equals(message.getSubjectEntry())) {
                    // False crash of this node detected
                    System.out.println("\nFalse positive crash of this node detected. Stopping execution.\n");
                    logger.warning("False positive crash of this node detected. Stopping execution.");

                    // Leave group silently
                    leaveGroup(false);

                    // Command prompt
                    System.out.print("MemberProcess$ ");
                    break;
                }
                synchronized (memberList) {
                    if (memberList.removeEntry(message.getSubjectEntry())) {
                        logger.info("Process removed from membership list: " + message.getSubjectEntry());
                    }
                }
                // If coordinator crashed
                if (memberList.getCoordinator() == null) {
                    if (memberList.hasSuccessor()) {
                        // Start election if needed
                        if(!electionOngoing.get()) {
                            electionOngoing.set(true);
                            sendMessageToSuccessor(new MembershipMessage(MessageType.ElectionId, selfEntry));
                        }
                    } else {
                        // If no successors exist, this process is automatically the new coordinator
                        memberList.updateCoordinator(selfEntry);
                        coordinator = new Coordinator(memberList, end);
                    }
                }
                break;

            case ElectionId:
                // Ignore further election messages if coordinator already exists.
                // Eventually the failure of coordinator will be known at all nodes and election will proceed
                if (memberList.getCoordinator() != null) {
                    break;
                }

                electionOngoing.set(true);

                if (selfEntry.equals(message.getSubjectEntry())) {
                    // This process has been elected
                    memberList.updateCoordinator(selfEntry);
                    coordinator = new Coordinator(memberList, end);
    
                    sendMessageToSuccessor(new MembershipMessage(MessageType.Elected, selfEntry));
                }
                else if (selfEntry.compareTo(message.getSubjectEntry()) > 0 || !memberList.containsEntry(message.getSubjectEntry())) {
                    // Send own ID if higher or failure detected of message subject
                    sendMessageToSuccessor(new MembershipMessage(MessageType.ElectionId, selfEntry));
                } else {
                    // Forward message otherwise
                    sendMessageToSuccessor(message);
                }
                break;

            case Elected:
                if (memberList.getCoordinator() != null) {
                    // Ignore further election messages once new coordinator is known
                    break;
                } else {
                    // Update coordinator and forward message
                    memberList.updateCoordinator(message.getSubjectEntry());
                    sendMessageToSuccessor(message);
                }

                electionOngoing.set(false);

                break;

            // Do nothing
            case IntroducerCheckAlive:
            default:
                break;
        }
    }

    // Process file server related messages
    private void processFileMessage(FileMessage message, Socket client, ObjectInputStream input, ObjectOutputStream output) {
        // Send to appropriate server (Coordinator or FileServer)
        switch (message.getDestination()) {
            case FileServer:
                fileServer.processFileMessage(message, client, input, output);
                break;
            case Coordinator:
                assert coordinator != null: "Received message for coordinator while this node is not the coordinator";
                coordinator.processFileMessage(message, client, input, output);
                break;
            case Client:
                assert false: "TCP server recieved message meant for an existing connection";
            default:
                break;
        }
    }

    // For receiving UDP messages and responding
    // For sending pings and checking ack
    private void mainProtocol() {
        try {
            socket = new DatagramSocket(selfEntry.getPort());
            // Maintain list of acknowledgements to know which member sent the ACK
            List<AtomicBoolean> ackSignals = new ArrayList<>(NUM_MONITORS);
            for (int i = 0; i < NUM_MONITORS; i++) {
                ackSignals.add(new AtomicBoolean());
            }
            // Receive ping and send ACK
            Receiver receiver = new Receiver(socket, selfEntry, end, ackSignals);
            receiver.setDaemon(true);
            receiver.start();
            logger.info("UDP Socket opened");
            while(!end.get()) {
                List<MemberListEntry> successors;
                synchronized (memberList) {
                    // Get the next successors to send ping to
                    successors = memberList.getSuccessors(NUM_MONITORS);
                    // Update receiver about the successor information
                    receiver.updateAckers(successors);
                    // Send ping
                    for (int i = 0; i < successors.size(); i++) {
                        new SenderProcess(ackSignals.get(i), successors.get(i), 500).start();
                    }
                }
                //Wait for protocol time period
                Thread.sleep(PROTOCOL_TIME);
            }
            socket.close();
            logger.info("UDP Socket closed");
            receiver.join();
        }catch (SocketException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Process to send Ping and wait for ACK
    private class SenderProcess extends Thread {
        public MemberListEntry member;
        private AtomicBoolean ackSignal;
        private long timeOut;

        private void ping(MemberListEntry member, MemberListEntry sender) throws IOException {
            MembershipMessage message = new MembershipMessage(MembershipMessage.MessageType.Ping, sender);
            UDPProcessing.sendPacket(socket, message, member.getHostname(), member.getPort());
        }

        public SenderProcess(AtomicBoolean ackSignal, MemberListEntry member, long timeOut)  {
            this.member = member;
            this.ackSignal = ackSignal;
            this.timeOut = timeOut;
        }

        @Override
        public void run() {

            try {
                // Ping successor
                synchronized (ackSignal) {
                    ackSignal.set(false);
                    logger.info("Pinging " + member);
                    ping(member, selfEntry);
                    ackSignal.wait(timeOut);
                }

                // Handle ACK timeout
                if (!ackSignal.get()) {
                    logger.warning("ACK not received from " + member);
                    logger.warning("Process failure detected detected: " + member);
                    // Disseminate message first in case of false positive
                    disseminateMessage(new MembershipMessage(MembershipMessage.MessageType.Crash, member));

                    // Then remove entry
                    synchronized (memberList) {
                        if (memberList.removeEntry(member)) {
                            logger.info("Process removed from membership list: " + member);
                        }
                    }

                    // If coordinator crashed
                    if (memberList.getCoordinator() == null) {
                        if (memberList.hasSuccessor()) {
                            // Start election if needed
                            if(!electionOngoing.get()) {
                                electionOngoing.set(true);
                                sendMessageToSuccessor(new MembershipMessage(MessageType.ElectionId, selfEntry));
                            }
                        } else {
                            // If no successors exist, this process is automatically the new coordinator
                            memberList.updateCoordinator(selfEntry);
                            coordinator = new Coordinator(memberList, end);
                        }
                    }
                } else {
                    logger.info("ACK received from " + member);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
