import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;
import static java.lang.Integer.parseInt;

public class Coordinator extends Thread {

    private static Integer port = 0;        // port of coordinator
    private static Integer num_participants = 0;        // total number of participants
    private static ArrayList<String> options = new ArrayList<String>();     // arraylist of vote options
    private ArrayList<String> outcomes = new ArrayList<String>();           // arraylist to store outcomes received
    private HashSet<String> voters = new HashSet<String>();                 // set to store participants that have voted

    // hashmap to store participant port and corresponding printwriter
    private Map<Integer, PrintWriter> participantsList = Collections.synchronizedMap(new HashMap<Integer, PrintWriter>());

    public static void main(String args[]) {
        // Getting command line arguments
        for (int i = 0; i < args.length; i++) {
            if (i == 0)
                port = parseInt(args[i]);
            else if (i == 1)
                num_participants = parseInt(args[i]);
            else
                options.add(args[i]);
        }
        new Coordinator().start();
    }

    public void run(){ startListening(); }

    public void startListening(){
        // Coordinator starts listening for connections
        try {
            System.out.println("Coordinator has started");
            ServerSocket listener = new ServerSocket(port);
            while (true) {
                Socket client = listener.accept();
                new ServerThread(client).start();
            }
        } catch (Exception e) { System.out.println("Unexpected Error: " + e);}
    }

    private class ServerThread extends Thread {
        private Socket client;
        private BufferedReader in;
        private PrintWriter out;

        ServerThread(Socket client) throws IOException {
            this.client = client;
            // Open IO streams
            in = new BufferedReader(new InputStreamReader(client.getInputStream()));
            out = new PrintWriter(new OutputStreamWriter(client.getOutputStream()));
        }

        public void run() {
            // Thread to handle coordinator participant communication
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] substrings = line.split(" ");
                    if (substrings[0].equals("JOIN") && substrings[1].matches("[0-9]+") && substrings.length == 2) {
                        // Participant JOIN message processed
                        System.out.println("Participant " + substrings[1] + " has joined");
                        JOIN(parseInt(substrings[1]), out);
                    } if (substrings[0].equals("OUTCOME")) {
                        // OUTCOME message from participants is processed
                        System.out.println(substrings[1] + " was the outcome according to a participant");
                        addToOutcomesList(substrings[1]);
                        addVoters(substrings);
                        if (checkIfAllOutcomesReceived()) {
                            System.out.println("Consensus was reached by all the participants and " + substrings[1] + " was the outcome");
                            System.out.println("Participants that took part in the voting: " + votersString());
                            System.exit(0);     // Close coordinator after outcome has been computed
                        }
                    } if (substrings[0].equals("TIE")) {
                        // TIE message from participants is processed
                        System.out.println("There was a tie, no consensus was reached");
                        System.out.println("Restarting consensus protocol with fewer vote options");
                        while (true) {
                            if (num_participants == 1){
                                System.out.println("There was a tie, no consensus was reached and one only participant is running");
                                System.out.println("Therefore, the outcome will be " + options.get(randomIndex(0,options.size())));
                                break;
                            } if (options.size() == 1) {
                                break;
                            } else {
                                reduceVoteOptions();
                                sendVoteOptions();
                            }
                        }
                    } if (substrings[0].matches("[0-9]+")){
                        // Participant sends its port to the coordinator when it fails
                        num_participants -= 1;
                        System.out.println("Participant " + substrings[0] + " has failed");
                    }
                }
                client.close();
            } catch (Exception e) {}
        }
    }

    private synchronized void JOIN(Integer n, PrintWriter out) {
        // Method to add joined participants into a hashmap
        participantsList.put(n, out);
        if (checkAllParticipantsJoined()) {
            // Once all participants have joined, details and vote options are sent to all participants
            System.out.println("All participants have joined");
            sendDetails();
            sendVoteOptions();
        }
    }

    private synchronized boolean checkAllParticipantsJoined(){
        // Method to check if all participants have joined
        return (participantsList.size() == num_participants);
    }

    private synchronized boolean checkIfAllOutcomesReceived(){
        // Method to check if all participants have sent their outcomes
        return (outcomes.size() == num_participants);
    }

    private synchronized void sendDetails(){
        // Method to send details to all participants
        ArrayList<Integer> tempParticipantsList = new ArrayList<Integer>(participantsList.keySet());
        for (int i = 0; i < tempParticipantsList.size(); i++) {
            sendDetailsToParticipant(tempParticipantsList.get(i));
        }
        System.out.println("Details sent to all participants");
    }

    private synchronized void sendVoteOptions(){
        // Method to send vote options to all participants
        ArrayList<Integer> tempParticipantsList = new ArrayList<Integer>(participantsList.keySet());
        for (int i = 0; i < tempParticipantsList.size(); i++) {
            sendVoteToParticipant(tempParticipantsList.get(i));
        }
        System.out.println("Vote options sent to all participants");
    }

    private synchronized void sendDetailsToParticipant (Integer port) {
        // Method to send details to participant with particular port
        ArrayList<Integer> tempParticipantsList = new ArrayList<Integer>(participantsList.keySet());

        String detailsString = "DETAILS ";

        for (int i = 0; i < tempParticipantsList.size(); i++) {
            if (tempParticipantsList.get(i) != port && i == tempParticipantsList.size() - 1)
                detailsString += tempParticipantsList.get(i);
            else if (tempParticipantsList.get(i) != port)
                detailsString += tempParticipantsList.get(i) + " ";
        }

        PrintWriter out = participantsList.get(port);

        out.println(detailsString);
        out.flush();
        System.out.println("Details of other participants sent to " + port);
    }

    private synchronized void sendVoteToParticipant (Integer port) {
        // Method to send vote options to participant with particular port
        String vote_optionsString = "VOTE_OPTIONS " + options.stream().collect(Collectors.joining(" "));

        PrintWriter out = participantsList.get(port);

        out.println(vote_optionsString);
        out.flush();
        System.out.println("Vote options sent to " + port);
    }

    private synchronized void addToOutcomesList(String str) {
        // Method to add outcomes to a list
        outcomes.add(str);
    }

    private synchronized void addVoters(String[] substrings){
        // Add all participants that voted to a set
        for (int i = 2; i < substrings.length; i++)
                voters.add(substrings[i]);
    }

    private synchronized String votersString(){
        // Create string that contains all participants have voted
        ArrayList<String> tempList  = new ArrayList<String>(voters);
        Collections.sort(tempList);

        String votersStr = "";

        for (int i = 0; i < tempList.size(); i++) {
            if (i == tempList.size() - 1)
                votersStr += tempList.get(i);
            else
                votersStr += tempList.get(i) + " ";
        }
        return votersStr;
    }

    private synchronized void reduceVoteOptions(){
        // Method to reduce vote options. Called in case of a tie
        int i = randomIndex(0, options.size()-1);
        options.remove(i);
    }

    private synchronized int randomIndex(Integer min, Integer max) {
        // General method to return a random index from a specified range
        Random random = new Random();
        return random.nextInt((max - min) + 1) + min;
    }
}
