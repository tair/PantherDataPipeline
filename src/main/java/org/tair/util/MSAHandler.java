package org.tair.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class MSAHandler extends DefaultHandler {

    private List<String> sequenceList = new ArrayList<>();

    private StringBuilder annotation_node_id;
    private StringBuilder full_sequence;

    private Map<String,String> sequenceInfo;

    private boolean bSequenceInfo = false;
    private boolean bFullSequence = false;
    private boolean bId = false;

    private ObjectWriter ow = new ObjectMapper().writer();

    public List<String> getSequenceInfo(){

        return sequenceList;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {

        if (qName.equalsIgnoreCase("sequence_info")) {
            sequenceInfo= new HashMap<>();
            bSequenceInfo = true;
        } else if (qName.equalsIgnoreCase("annotation_node_id")) {
            annotation_node_id = new StringBuilder();
            bId = true;
        } else if (qName.equalsIgnoreCase("full_sequence")) {
            full_sequence = new StringBuilder();
            bFullSequence = true;
        }
    }


    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (qName.equalsIgnoreCase("sequence_info")) {
            try {
                sequenceList.add(ow.writeValueAsString(sequenceInfo));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            bSequenceInfo = false;
        } else if (qName.equalsIgnoreCase("annotation_node_id")) {
            sequenceInfo.put("annotation_node_id", annotation_node_id.toString());
            bId = false;
        } else if (qName.equalsIgnoreCase("full_sequence")) {
            sequenceInfo.put("full_sequence", full_sequence.toString());
            bFullSequence = false;
        }
    }


    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {

        if (bId && bSequenceInfo) {
            annotation_node_id.append(new String(ch, start, length));
        } else if (bFullSequence && bSequenceInfo) {
            full_sequence.append(new String(ch, start, length));
        }
    }
}