/**
 * <p>Title: SubmitterDocument</p>
 *
 * <p>Description: this is the data structure for storing the information of a crawled document</p>
 *
 * <p>Copyright: Copyright Madian Khabsa @ Penn State(c) 2009</p>
 *
 * <p>Company: Penn State</p>
 *
 * @author Madian Khabsa
 * @version 1.0
 */
package edu.psu.ist.youseer;

public class SubmitterDocument {

    /**
     *  The binary content of any not text document
     */
    private byte[] ByteContent;
    /**
     *  The title of the document
     */
    private String Title;
    /**
     * i.e. text/html, text/xml ...etc
     */
    private String DataType;
    /**
     * The URL of the document
     */
    private String Url;
    /**
     * The text content of the file before stripping
     */
    private String RawTextContent;
    /**
     * The file content after stripping the HTML tags
     */
    private String StrippedTextContent;
    /**
     * offset within the ARC file
     */
    private int Offset;
    /**
     *  The absolute path of the ARC file that contains this document
     */
    private String ContainingFile; 



    public SubmitterDocument(String Url, String dataType,
                             String containingFile, int recordOffset) {
        this.Url = Url;
        this.DataType = dataType;
        this.Offset = recordOffset;
        this.ContainingFile = containingFile;
    }


    public SubmitterDocument(String Url, String RawTextContent, String dataType, String containingFile, int recordOffset) {
        this.Url = Url;
        this.RawTextContent = RawTextContent;
        this.DataType = dataType;
        this.Offset=recordOffset;
        this.ContainingFile=containingFile;
    }

    public SubmitterDocument(String Url, byte[] buffer,String dataType, String containingFile,int recordOffset) {
        this.Url = Url;
        this.ByteContent = buffer;
        this.DataType = dataType;
        this.Offset = recordOffset;
        this.ContainingFile = containingFile;
    }

    public String getRawTextContent() {
        return RawTextContent;
    }

    public void setRawTextContent(String content){
        this.RawTextContent = content;
    }

    public void setTitle(String Title) {
        this.Title = Title;
    }

    public String getTitle() {
        return Title;
    }

    public void setDataType(String DataType) {
        this.DataType = DataType;
    }

    public String getDataType() {
        return DataType;
    }

    public String getUrl() {

        return Url;
    }



    public void setStrippedTextContent(String StrippedTextContent) {
        this.StrippedTextContent = StrippedTextContent;
    }

    public String getStrippedTextContent() {
        return StrippedTextContent;
    }

    public byte[] getByteContent() {
        return ByteContent;
    }

    public void setByteContent(byte[] buffer) {
    this.ByteContent = buffer;
}


    public int getOffset() {
        return Offset;
    }

    public String getContainingFile() {
        return ContainingFile;
    }
}
