package at.jku.dke.dwh.enron;

import com.google.common.base.Splitter;

import java.io.Serializable;
import java.time.LocalDateTime;

public class Email implements Serializable {
    private String from;
    private String to;
    private LocalDateTime timestamp;
    private String body;

    public Email(String from, String to, LocalDateTime timestamp, String body) {
        this.from = from;
        this.to = to;
        this.timestamp = timestamp;
        this.body = body;
    }

    @Override
    public String toString() {
        return "From: " + this.from + ", To: " + this.to + ", Timestamp: " + this.timestamp;
    }

    public String getBody() {
        return this.body;
    }

    public String getFrom() {
        return this.from;
    }
}
