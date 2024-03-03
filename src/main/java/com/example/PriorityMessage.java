package com.example;

public class PriorityMessage implements Comparable<PriorityMessage>{
    private Integer rank;
    private Long time; 
    private Message message;
    
    public PriorityMessage(Integer rank, Long time, Message message) {
        this.rank = rank;
        this.time = time;
        this.message = message;
    }

    public Integer getRank() {
        return rank;
    }

    public void setFirst(Integer rank) {
        this.rank = rank;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }


    public Message getMesssage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
    
    @Override
    public int compareTo(PriorityMessage other) {
        // Higher rank messages should come after lower rank messages
        int rankComparison = Integer.compare(this.rank, other.getRank());
        if (rankComparison == 0) {
            return Long.compare(this.time, other.time);
        }
        return rankComparison;
    }
}
