package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by Abhishek on 1/31/16.
 */
public class Message implements Serializable {
    public String type;
    public UUID uid;
    public int sid;
    public String body;
    public String port;
    public String status;
    public String destination;

    public String key;
    public String value;

    Message(String type) {
        this.type = type;
        status = "x";
    }


    public void setType(String type) {
        this.type = type;
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

    public void setSeqNo(int sid) {
        this.sid = sid;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public void setStatus(String status) {
        this.status = status;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getType() {
        return type;
    }


    public UUID getUid() {
        return uid;
    }

    public int getSeqNo() {
        return sid;
    }

    public String getBody() {
        return body;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }


    public String getPort() {
        return port;
    }

    public String getStatus() {
        return status;
    }

    public String getDestination() {
        return destination;
    }


}
