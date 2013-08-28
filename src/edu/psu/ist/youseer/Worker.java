package edu.psu.ist.youseer;

import org.archive.io.arc.ARCRecord;
import net.htmlparser.jericho.MicrosoftTagTypes;
import java.io.Reader;
import java.io.InputStream;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tika.parser.ParsingReader;
import net.htmlparser.jericho.Source;
import java.io.ByteArrayInputStream;
import org.apache.tika.parser.AutoDetectParser;
import net.htmlparser.jericho.PHPTagTypes;
import net.htmlparser.jericho.MasonTagTypes;
import org.apache.tika.parser.Parser;
import org.apache.solr.analysis.HTMLStripReader;
import java.io.StringReader;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import java.io.IOException;
import net.htmlparser.jericho.CharacterReference;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.HttpClient;
import org.archive.io.arc.ARCRecordMetaData;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.sql.Statement;
import java.util.Date;
import java.text.Format;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.Timestamp;/**
 * <p>Title: </p>
 *
 * <p>Description: This is the basic unit of execution, each worker is responsible for parsing a document and generating
 * the corresponding solr document. During the processing, the SubmitterDocument is passed to the CustomeExtractor to
 * see if the user has implemented some specific extraction functions.</p>
 *
 * <p>Copyright: Copyright Madian Khabsa @ Penn State(c) 2009</p>
 *
 * <p>Company: Penn State</p>
 *
 * @author Madian Khabsa
 * @version 1.0
 */
public class Worker implements Runnable{
    private ARCSubmitter parent;

    private SubmitterDocument doc;

    public Worker(ARCSubmitter parent, SubmitterDocument doc) {
        this.parent = parent;

        this.doc = doc;
    }

    public void run()
    {
        try {
            String content = "";


            if (this.doc.getDataType().startsWith("text")) {

                this.ProcessTextDocument();
            } else {

                this.ProcessBinaryDocument();
            }

            //System.err.println("Submitting File: " +
            //                   this.doc.getContainingFile() + ": " + this.doc.getUrl());
            //System.out.println("Submitting File: " +
             //                  this.doc.getContainingFile() + ": " + this.doc.getUrl());
            String toSend = this.GenerateDocument();
            String result = this.sendPostCommand(toSend,
                    this.parent.Config.IndexURL);
            //System.out.println("The document is: " + toSend);

            //this.parent.IncrementCount();
            this.parent.IndexedDocs.add(this.doc);
            // Insert the result to the DB
            //this.InsertToDB("0");

        }

        catch (Exception ex)
        {
            System.err.println("Exception encountered while processin the URL:" + this.doc.getUrl());
            System.err.println("Error Message:" + ex.getMessage());
            ex.printStackTrace();
            this.InsertToDB(ex.getMessage());

            // Insert the failure to the DB
        }

    }

    /**
     * Inserts a log entry to the database that the current document wasn't submitted to the index
     * @param result String The exception error message
     * @return boolean
     */
    private boolean InsertToDB(String result)
{
    try {
        Class.forName(this.parent.Config.DatabaseProvider);
        Connection myConn = DriverManager.getConnection(
                this.parent.Config.DBConnectionString);
        Statement stat = myConn.createStatement();
        PreparedStatement prep = myConn.prepareStatement(
                "insert into SubmissionErrors (Url, IndexingTime ,FileType , ContainingFile ,RecordOffset , ErrorMessage) values (?, ?, ?, ?, ?, ?);");

        java.util.Date fromDate = new java.util.Date();
        prep.setString(1, this.doc.getUrl());
        prep.setTimestamp(2, new Timestamp(fromDate.getTime()));
        prep.setString(3, this.doc.getDataType());
        prep.setString(4, this.doc.getContainingFile());
        prep.setInt(5, this.doc.getOffset());
        prep.setString(6, result);
        prep.addBatch();
        prep.executeBatch();
        return true;


    } catch (Exception e) {
        System.err.println(e.getMessage());
        e.printStackTrace();
        return false;
    }
}

/**
 * Processes the text document, extracts the title, and strip the HTML tags
 * @return boolean
 */
public boolean ProcessTextDocument() {
       // The content should be in plain HTML, prefered not to be stripped
       String withoutHTML = this.StripHTML(doc.getRawTextContent());
       withoutHTML = StringEscapeUtils.escapeXml(withoutHTML);
       doc.setStrippedTextContent(withoutHTML);
       String title = ""; //doc.TitleProperty;

       MicrosoftTagTypes.register();
       PHPTagTypes.register();
       PHPTagTypes.PHP_SHORT.deregister(); // remove PHP short tags for this example otherwise they override processing instructions
       MasonTagTypes.register();
       Source source = new Source(doc.getRawTextContent());
       source.fullSequentialParse();

       title = getTitle(source);

       if (title != null) {

           title = StringEscapeUtils.escapeXml(title);
           doc.setTitle(title);

       }

       return true;
   }

   /**
    * Process the bindary document, converts it to plain text using apache tika, and then extracts the title of the file
    * @return boolean
    */
   public boolean ProcessBinaryDocument() {
       // The content should be in plain HTML, prefered not to be stripped
       try{
           InputStream is = new ByteArrayInputStream(doc.getByteContent());
           Parser prsr = new AutoDetectParser();
           org.apache.tika.metadata.Metadata md = new org.apache.tika.metadata.
                   Metadata();
           Reader reader = new ParsingReader(prsr, is, md);
           doc.setTitle(md.get(md.TITLE));
           int read = 0;
           StringBuilder sb = new StringBuilder();
           do {

               char[] charArr = new char[4096];

               read = reader.read(charArr, 0, 4096);

               sb.append(charArr, 0, read == -1 ? 0 : read);

           } while (read != -1);
           reader.close();
           is.close();
           doc.setStrippedTextContent(StringEscapeUtils.escapeXml(sb.toString()));

           return true;
       }
       catch (Exception e)
       {
           System.err.println("Exception encountered while processing binary document" + e.getMessage());
           e.printStackTrace();
                   return false;
       }



   }

   /**
    * Generates solr document for the processed ARC record using the tags from the configuration file
    * @param doc SubmitterDocument
    * @return String
    */

  public String GenerateDocument() {

      StringBuilder command = new StringBuilder();
      command.append("<add>").append(ARCSubmitter.LINE_SEP).append("<doc>")
              .append("<field name=\""+ this.parent.Config.URL +"\">").append(doc.getUrl()).append("</field>").
              append(ARCSubmitter.LINE_SEP);

      String relativePath = this.parent.CacheFolder + doc.getContainingFile().substring(this.parent.OrgiginalPart.length());
      command.append("<field name=\""+ this.parent.Config.CACHE +"\">").append(relativePath).append(
              "</field>").append(ARCSubmitter.LINE_SEP);

      command.append("<field name=\"" + this.parent.Config.OFFSET +
                     "\">").append(doc.getOffset()).append("</field>").
              append(ARCSubmitter.LINE_SEP);

      command.append("<field name=\"" + this.parent.Config.DOCUMENT_TEXT +
                     "\">").append(doc.getStrippedTextContent()).
              append("</field>").append(ARCSubmitter.LINE_SEP);

      command.append("<field name=\"" + this.parent.Config.FILE_TYPE + "\">").
              append(doc.getDataType()).append("</field>").append(ARCSubmitter.
              LINE_SEP);

      if (doc.getTitle() != null) {

          command.append("<field name=\"" + this.parent.Config.TITLE +
                         "\">").append(doc.getTitle()).append(
                                 "</field>").append(ARCSubmitter.LINE_SEP);

      }

      String customData = Extractor.GenerateCustomeData(doc);
      if (customData != "")
          command.append(customData);

      command.append("</doc>").append(ARCSubmitter.LINE_SEP);
      command.append("</add>").append(ARCSubmitter.LINE_SEP);
      return command.toString();

  }
  /**
   * Strips the text from the HTML tags. This is dependent on the class HTMLStripReader that comes as part of solr.
   * @param rawString String
   * @return String
   */
  public String StripHTML(String rawString) {
     StringReader sr = new StringReader(rawString);
     HTMLStripReader reader = new HTMLStripReader(sr);
     try {
         int i = 0;
         StringBuilder sb = new StringBuilder();
         do {
             char[] arr = new char[4096];
             i = reader.read(arr, 0, 4096);
             String str = new String(arr, 0, i == -1 ? 0 : i);

             str = str.replaceAll("[\\s\\s]+", " ");

             sb.append(str);

         } while (i != -1);

         return sb.toString();

     } catch (Exception ex) {
         System.err.println("Exception At HTMLStripper" + ex.getMessage());
         ex.printStackTrace();
         return "";
     }
 }

 /**
    * Sends a post request to the server
    * Courtesy of Grant Ingersoll @ IBM
    * @param command String the command to be sent
    * @param url String the URL of the server
    * @return String The result of the submit
    * @throws Exception
     */
 public String sendPostCommand(String command, String url) throws
            Exception {
        String results = null;
        HttpClient client = new HttpClient();
        PostMethod post = new PostMethod(url);

        RequestEntity re = new StringRequestEntity(command, "text/xml", "UTF-8");
        post.setRequestEntity(re);
        try {
            // Execute the method.
            int statusCode = client.executeMethod(post);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + post.getStatusLine());
            }

            // Read the response body.
            byte[] responseBody = post.getResponseBody();

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            results = new String(responseBody);
        } catch (HttpException e) {
            //System.err.println("Fatal protocol violation: " + e.getMessage());
            //e.printStackTrace();
            throw e;
        } catch (Exception e) {
            //System.err.println("Fatal transport error: " + e.getMessage());
            //e.printStackTrace();
            throw e;
        } finally {
            // Release the connection.
            post.releaseConnection();
            client.getHttpConnectionManager().closeIdleConnections(0); // Don not remove this line
            // releaseConnection() is known to throw an exception, and leave the connection open, hence increase the possibility
            // of file descriptors leak in the system. Eventually exhasting all the open files limit
            // closeIdleConnections() will incure some overhead, but you won't risk running out of file descriptos


        }
        return results;
    }

    /**
     *  Extracts the title out of a text document using Jericho parser
     * @param source Source
     * @return String
     */
    private static String getTitle(Source source) {
        net.htmlparser.jericho.Element titleElement = source.getFirstElement(
                net.htmlparser.jericho.HTMLElementName.TITLE);
        if (titleElement == null)
            return null;
        // TITLE element never contains other tags so just decode it collapsing whitespace:
        return CharacterReference.decodeCollapseWhiteSpace(titleElement.
                getContent());
    }


}
