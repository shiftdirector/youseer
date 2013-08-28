/**
 * <p>Title: Extractor</p>
 *
 * <p>Description: The user implements this class to extract application specific data. There is a single method
 * in this class that will be called, GenerateCustomeData, which will have the SubmitterDocument object as input.
 * The output should be in XML format, like this:
 * <field name="page_author">YOUR NAME</field>

 *
 * </p>
 *
 * <p>Copyright: Copyright Madian Khabsa @ Penn State(c) 2009</p>
 *
 * <p>Company: Penn State</p>
 *
 * @author Madian Khabsa
 * @version 1.0
 */
package edu.psu.ist.youseer;
public class Extractor {
    public Extractor() {
    }

    public static String GenerateCustomeData(SubmitterDocument doc)
    {
        // Add your code here
        return "";
    }

    /*
         Example Code:
     public static String GenerateCustomeData(SubmitterDocument doc)
         {

     StringBuilder command = new StringBuilder();
      command.append("<field name=\"page_author\">").append("Your Name").append("</field>").
              append(ARCSubmitter.LINE_SEP);

        return command.toString();;
         }
     * 
     *
     * public static String GenerateCustomeData1(SubmitterDocument doc)
  {
	StringBuilder command = new StringBuilder();
                 String url = doc.getUrl();
String date = url.substring(23, 27) + "-" + url.substring(28, 30) + "-" + url.substring(31, 33) + "T00:00:00Z";
	command.append("<field name=\"publication_date\">").append(date).append("</field>").
append(ARCSubmitter.LINE_SEP);
return command.toString();
}


     */
 public static String GenerateCustomeData1(SubmitterDocument doc)
         {

     StringBuilder command = new StringBuilder();
      command.append("<field name=\"page_author\">").append("Your Name").append("</field>").
              append(ARCSubmitter.LINE_SEP);

        return command.toString();
         }



}
