import twitter4j.GeoLocation;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterException;

import java.io.IOException;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.List;

public class Twitterer{
      private Twitter twitter;
      private PrintStream consolePrint;
      private List<Status> statuses;

      public Twitterer(PrintStream console){
         //makes an instance of Twitter
         twitter = TwitterFactory.getSingleton();
         consolePrint = console;
         statuses = new ArrayList<Status>();
      }

      //tweets a given message
      public void tweetOut(String message) throws TwitterException, IOException
      {
        //this is what tweets !!!!
        Status status = twitter.updateStatus(message);
        //confirming the tweet sent out to console
        System.out.println("Succesfully Tweeted: " + status.getText());
      }
   }
