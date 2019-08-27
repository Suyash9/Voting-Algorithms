import java.io.*;
import java.net.*;
import java.util.*;
import static java.lang.Integer.parseInt;

public class Participant extends Thread{

    private static Integer cport = 0;       // port of coordinator
    private static Integer pport = 0;       // port of participant
    private static Integer timeout = 0;     // timeout of participant
    private static Integer failurecond = 0;     // failure condition of participant
    private ArrayList<Integer> otherParticipants = new ArrayList<Integer>();        // list of other participants
    private ArrayList<String> options = new ArrayList<String>();            // list of vote options
    private HashMap<Integer, String> votes = new HashMap<Integer, String>();        // map of ports to their respective votes
    private String participantVote;                     // vote of this participant

    public static void main(String args[]){
        // Getting command line arguments
        for(int i=0;i<args.length;i++){
            if(i==0)
                cport = parseInt(args[i]);
            else if(i==1)
                pport = parseInt(args[i]);
            else if(i==2)
                timeout = parseInt(args[i]);
            else if(i==3)
                failurecond = parseInt(args[i]);
        }
        new Participant().start();
    }

    public void run(){ connectToCoordinator(); }

    private synchronized void connectToCoordinator(){
        // Method to establish connection with coordinator
        try {
            Socket socket = new Socket(InetAddress.getLocalHost(), cport);
            new CoordinatorThread(socket).start();
        } catch (ConnectException e) { System.out.println("Start coordinator before starting participant");
        } catch (Exception e) {System.out.println("Unexpected Error: " + e);}
    }

    private class CoordinatorThread extends Thread{
        private Socket socket;
        private BufferedReader in;
        private PrintWriter out;

        CoordinatorThread(Socket socket) throws IOException{
            this.socket = socket;
            // Open IO streams
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
        }

        public void run() {
            // Thread for communication with coordinator
            try {
                System.out.println("Participant " + pport + " has started");
                // Send JOIN message to coordinator
                participantJoin(out);

                String line;
                while ((line = in.readLine()) != null) {
                    String[] substrings = line.split(" ");
                    if (substrings[0].equals("DETAILS")) {
                        // DETAILS message from coordinator is processed
                        addOtherParticipants(substrings);       // Processing DETAILS message
                        System.out.println("Details of other participants received");

                        String others = "Other participants: ";
                        for (int i = 1; i < substrings.length; i++) {
                            if (i == substrings.length - 1)
                                others += substrings[i];
                            else
                                others += substrings[i] + " ";
                        }
                        System.out.println(others);
                    } else if (substrings[0].equals("VOTE_OPTIONS")) {
                        // VOTE_OPTIONS message from coordinator is processed
                        addVoteOptions(substrings);         // Processing VOTE_OPTIONS message

                        String voteoptions = "Vote options: ";
                        for (int i = 1; i < substrings.length; i++) {
                            if (i == substrings.length - 1)
                                voteoptions += substrings[i];
                            else
                                voteoptions += substrings[i] + " ";
                        }
                        if (!options.isEmpty()) {
                            System.out.println("Vote options received");
                            System.out.println(voteoptions);
                        }

                        setParticipantVote();           // Setting vote for this participant
                        new ListenerThread().start();   // Start thread for communication with other participants
                        sleep(1000);
                        sendVotesRound1();              // Voting Round 1
                        sleep(1000);
                        sendVotesRound2();              // Voting Round 2

                        if (failurecond == 0) {
                            sleep(1000);
                            sendOutcome(out);           // Send outcome to coordinator
                            if (outcomeString().equals("TIE")){
                                // If there is a tie, the options and votes are cleared so that new ones can be received
                                options.clear();
                                votes.clear();
                            } else{
                                // Otherwise exit once outcome has been sent to coordinator
                                socket.close();
                            }
                        } else{
                            // Socket is closed if participant failed
                            System.out.println("Participant " + pport + " failed");
                            out.println(pport);
                            out.flush();
                            socket.close();
                        }
                    }
                }
                socket.close();
            } catch (SocketException e){
                System.out.println("Participant " + pport + " socket closed");
                System.exit(0);
            } catch (Exception e){}
        }
    }

    private synchronized void participantJoin(PrintWriter out){
        // Method to send join message to participant
        String join = "JOIN " + pport;
        out.println(join);
        out.flush();
        System.out.println("Participant " + pport + " sent join message");
    }

    private synchronized void addOtherParticipants(String[] substrings) {
        // Add other participants to list of other participants
        for (int i = 1; i < substrings.length; i++)
            otherParticipants.add(parseInt(substrings[i]));
    }

    private synchronized void addVoteOptions(String[] substrings){
        // Add vote options to list of vote options
        for (int i = 1; i < substrings.length; i++)
            options.add(substrings[i]);
    }

    private synchronized void setParticipantVote(){
        // Method to set vote of this participant
        ArrayList<String> tempOptionsList = new ArrayList<String>(options);
        participantVote = getRandomElement(tempOptionsList);
    }

    private class ListenerThread extends Thread{
        // Thread that handles communication with other participants
        public void run(){
            try{
                ServerSocket ss = new ServerSocket(pport);
                ss.setSoTimeout(timeout);
                while(true){
                    try{
                        final Socket client = ss.accept();
                        new Thread(new Runnable(){
                            public void run(){
                                try{
                                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                    String line;

                                    while((line = in.readLine()) != null) {
                                        String[] substrings = line.split(" ");
                                        if(substrings[0].equals("VOTE") && substrings.length > 1) {
                                            // VOTE messages from other participants processed
                                            for(int i = 1; i < substrings.length; i++){
                                                if(substrings[i].matches("[0-9]+"))
                                                    addVotes(parseInt(substrings[i]), substrings[i+1]);
                                            }
                                        }
                                    }
                                    client.close();
                                } catch(Exception e){System.out.println("Unexpected Error: " + e);}
                            }
                        }).start();
                    }catch(SocketTimeoutException e){
                        ss.close();
                        System.out.println("Participant " + pport + " timed out");
                        break;
                    }
                }
            }catch(Exception e){}
        }
    }

    private synchronized void sendVotesRound1(){
        // Method to perform voting round 1
        System.out.println("Voting Round 1");
        if (failurecond == 1){
            int index = randomIndex(1, otherParticipants.size());
            for (int i = 0; i < index; i++)
                voteRound1(otherParticipants.get(i));
        }else {
            for (int i = 0; i < otherParticipants.size(); i++)
                voteRound1(otherParticipants.get(i));
        }
    }

    private synchronized void voteRound1(Integer port){
        // Method to send vote to participant with particular port in round 1 of voting
        try{
            Socket socket = new Socket(InetAddress.getLocalHost(), port);
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            String voteString = "VOTE " + pport.toString() + " " + participantVote;

            out.println(voteString);
            out.flush();
            System.out.println("Vote " + participantVote + " sent to " + port);

            socket.close();

        }catch (ConnectException e) { System.out.println("Participant " + port + " has timed out");
        }catch(Exception e){System.out.println("Unexpected Error: "+e);}
    }

    private synchronized void addVotes(Integer port, String vote){
        // Method to add votes to hashmap
        votes.put(port, vote);
    }

    private synchronized void sendVotesRound2(){
        // Method to perform voting round 2
        System.out.println("Voting Round 2");
        for (int i = 0; i < otherParticipants.size(); i++)
            voteRound2(otherParticipants.get(i));
    }

    private synchronized void voteRound2(Integer port){
        // Method to send vote to participant with particular port in round 2 of voting
        try{
            Socket socket = new Socket(InetAddress.getLocalHost(), port);
            PrintWriter out = new PrintWriter(socket.getOutputStream());

            ArrayList<Integer> participants = new ArrayList<Integer>(votes.keySet());

            String voteString = "";

            for (int i = 0; i < participants.size(); i++) {
                if (i == participants.size() - 1) {
                    voteString += participants.get(i) + " " + votes.get(participants.get(i));
                } else
                    voteString += participants.get(i) + " " + votes.get(participants.get(i)) + " ";
            }

            out.println("VOTE " + voteString);
            out.flush();
            System.out.println("Votes: " + voteString + " sent to " + port);

            socket.close();

        }catch (ConnectException e) { System.out.println("Participant " + port + " has timed out");
        }catch(Exception e){System.out.println("Unexpected Error: "+e);}
    }

    private synchronized String outcomeString(){
        // Method to create the OUTCOME message to send to the coordinator
        ArrayList<Integer> participants = new ArrayList<Integer>(votes.keySet());

        if (outcomeOption() == null){
            return "TIE";
        } else {
            String outcomeString = "OUTCOME " + outcomeOption() + " ";
            for (int i = 0; i < participants.size(); i++) {
                if (i == participants.size() - 1)
                    outcomeString += participants.get(i);
                else
                    outcomeString += participants.get(i) + " ";
            }
            return outcomeString;
        }
    }

    private synchronized String outcomeOption() {
        // Method to find the majority vote option
        ArrayList<String> tempVotes = new ArrayList<String>(votes.values());

        if (findMajority(tempVotes).size() > 1){
            return null;
        }else
            return (findMajority(tempVotes).get(0));
    }

    private synchronized void sendOutcome(PrintWriter out) {
        // Method to send OUTCOME message to the coordinator
        String[] substrings = outcomeString().split(" ");

        String others = "Participants that took part in voting: ";

        for (int i = 2; i < substrings.length; i++) {
            if (i == substrings.length - 1)
                others += substrings[i];
            else
                others += substrings[i] + " ";
        }

        if (outcomeOption() != null){
            System.out.println("Outcome according to " + pport + ": " + substrings[1]);
            System.out.println(others);
        } else
            System.out.println("There was a tie, no consensus was reached");

        out.println(outcomeString());
        out.flush();
    }

    private synchronized String getRandomElement(ArrayList<String> list) {
        // General method to return a random element from a list of strings
        Random rand = new Random();
        return list.get(rand.nextInt(list.size()));
    }

    private synchronized ArrayList<String> findMajority(ArrayList<String> list) {
        // General method to find majority element from a list of strings
        HashMap<String,Integer> countMap = new HashMap<String,Integer>() ;

        for (String str: list) {
            if (! countMap.containsKey(str)) {
                countMap.put(str, 1 ) ;
            } else {
                int value = countMap.get(str) ;
                value++ ;
                countMap.put(str, value) ;
            }
        }

        ArrayList<String> majority = new ArrayList<String>();

        for (Map.Entry<String, Integer> e : countMap.entrySet()) {
            if (e.getValue() == Collections.max(countMap.values() ))
                majority.add(e.getKey()) ;
        }
        return majority;
    }

    private synchronized int randomIndex(Integer min, Integer max) {
        // General method to return a random index from a specified range
        Random random = new Random();
        return random.nextInt((max - min) + 1) + min;
    }
}