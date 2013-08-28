package edu.psu.ist.youseer;
/**
 * <p>Title: SubmitterConfig</p>
 *
 * <p>Description: This is the configuration object of the submitter</p>
 *
 * <p>Copyright: Copyright Madian Khabsa @ Penn State(c) 2009</p>
 *
 * <p>Company: Penn State</p>
 *
 * @author Madian Khabsa
 * @version 1.0
 */


import java.util.*;
import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class SubmitterConfig {

    /**
     * The solr field that will store the URL
     */
    public String URL ;
    /**
     * The solr field that will store the title
     */
    public String TITLE ;
    /**
     * The solr field that will store the content
     */
    public String DOCUMENT_TEXT ;
    /**
     * The solr field that will store the file type of the document
     */
    public String FILE_TYPE ;
    /**
     * The solr field that will store the location of the cached version of this file
     */
    public String CACHE ;
    /**
     * The solr field that will store the offset within the ARC file
     */
    public String OFFSET;
    /**
     * Contains a set of all the Indexable file types, populated from the XML Configuration file
     */
    public HashSet  IndexedTypes = new HashSet();
    /**
     * The URL of the index
     */
    public String IndexURL ;
    /**
     * The path that will have the ARC files under it
     */
    public String CacheFolder ;
    /**
     * The absolute path of the root folder that contains the ARC files
     */
    public String OriginalPart ;
    /**
     * Database provider, read from the XML configuration file
     */
    public String DatabaseProvider ;
    /**
     * The database connection string, read from the XML configuration file
     */
    public String DBConnectionString ;

    public SubmitterConfig() {
    }


    public SubmitterConfig(String Url, String Title, String Document_Text,
                           String File_Type, String Cache, String offset) {

        this.CACHE = Cache;
        this.DOCUMENT_TEXT = Document_Text;
        this.FILE_TYPE = File_Type;
        this.TITLE = Title;
        this.URL = Url;
        this.OFFSET = offset;

    }

    /**
     * Reads the SubmitterConfig.xml file, and populates the data of this class. It validates the data before returning to ARCSubmitter
     * @param path the path to the configuration file
     * @return true if the XML file was read and parsed correctlly, and the content is valid, false otherwise
     */
    public boolean ReadConfigFile(String path) {
        try {
            File file = new File(path);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);
            doc.getDocumentElement().normalize();
            NodeList shemaConfigNodeLst = doc.getElementsByTagName(
                    "schemaConfiguration");

            NodeList indexedDataTypesNodeLst = doc.getElementsByTagName(
                    "indexedDataTypes");

            Node configNode = shemaConfigNodeLst.item(0);

            if (configNode.getNodeType() == Node.ELEMENT_NODE) {

                Element fstElmnt = (Element) configNode;
                NodeList fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "URL");
                Element fstNmElmnt = (Element) fstNmElmntLst.item(0);
                NodeList fstNm = fstNmElmnt.getChildNodes();
                this.URL =  ((Node) fstNm.item(0)).getNodeValue();

                fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "Title");
                fstNmElmnt = (Element) fstNmElmntLst.item(0);
                fstNm = fstNmElmnt.getChildNodes();
                this.TITLE = ((Node) fstNm.item(0)).getNodeValue();

                fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "Content");
                fstNmElmnt = (Element) fstNmElmntLst.item(0);
                fstNm = fstNmElmnt.getChildNodes();
                this.DOCUMENT_TEXT = ((Node) fstNm.item(0)).getNodeValue();

                fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "Type");
                fstNmElmnt = (Element) fstNmElmntLst.item(0);
                fstNm = fstNmElmnt.getChildNodes();
                this.FILE_TYPE = ((Node) fstNm.item(0)).getNodeValue();

                fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "Cache");
                fstNmElmnt = (Element) fstNmElmntLst.item(0);
                fstNm = fstNmElmnt.getChildNodes();
                this.CACHE = ((Node) fstNm.item(0)).getNodeValue();

                fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "Offset");
                fstNmElmnt = (Element) fstNmElmntLst.item(0);
                fstNm = fstNmElmnt.getChildNodes();
                this.OFFSET = ((Node) fstNm.item(0)).getNodeValue();

            }

            Node dataTypesNode = indexedDataTypesNodeLst.item(0);
            if ( dataTypesNode.getNodeType() == Node.ELEMENT_NODE)
            {
                Element rtElmnt = (Element) dataTypesNode;
                NodeList rtNmElmntLst = rtElmnt.getElementsByTagName(
                            "DataType");

                for (int s = 0; s < rtNmElmntLst.getLength(); s++) {

                    Node fstNode = rtNmElmntLst.item(s);
                     if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
                         Element fstElmnt = (Element) fstNode;
                         NodeList fstNm = fstElmnt.getChildNodes();
                         String val =  ((Node) fstNm.item(0)).getNodeValue();

                         this.IndexedTypes.add(val);
                     }
                }

            }

            NodeList dbConfigNodeLst = doc.getElementsByTagName(
                    "databaseConfiguration");
            Node dbNode = dbConfigNodeLst.item(0);
            if (dbNode.getNodeType() == Node.ELEMENT_NODE) {

                Element fstElmnt = (Element) dbNode;
                NodeList fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "provider");

                Element fstNmElmnt = (Element) fstNmElmntLst.item(0);
                NodeList fstNm = fstNmElmnt.getChildNodes();
                this.DatabaseProvider = ((Node) fstNm.item(0)).getNodeValue();

                fstNmElmntLst = fstElmnt.getElementsByTagName(
                        "connectionString");
                fstNmElmnt = (Element) fstNmElmntLst.item(0);
                fstNm = fstNmElmnt.getChildNodes();
                this.DBConnectionString = ((Node) fstNm.item(0)).getNodeValue();

            }


        } catch (Exception e) {
            System.err.println("Error in Reading Config file " + e.getMessage());
            return false;
        }

        return this.ValidateConfig();
    }

    /**
     * Validates this object after populating it from the XML configuration file.
     * 
     * @return ture if valid, false otherwise
     */
    private boolean ValidateConfig()
    {
        return this.CACHE.length() > 0 && this.DOCUMENT_TEXT.length() >0 && this.FILE_TYPE.length()>0 && this.IndexedTypes.size()>0
                && this.TITLE.length() > 0 && this.URL.length() > 0;
    }



}
