/**
 * Original idea by Ria Galanos, whose documentation and source can be found at
 * https://github.com/riagalanos/cs1-twitter
 **/

 //compile by    javac -cp .:./twitter4j-core-4.0.4.jar *.java
 //run by    java -cp .:./twitter4j-core-4.0.4.jar TwitterDriver
import twitter4j.TwitterException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

   public class TwitterDriver
   {
      private static PrintStream consolePrint;

      public static void main (String []args) throws TwitterException, IOException
      {
         // set up classpath and properties file
         Twitterer bigBird = new Twitterer(consolePrint);
         //message to tweet here

         String message = "I'm testing out the twitter4j API for Java.  Thanks @cscheerleader! ";
         bigBird.tweetOut(message);

      }
   }
