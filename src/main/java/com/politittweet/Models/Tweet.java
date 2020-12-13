package com.politittweet.Models;

public class Tweet
{
    int TweetId;
    String Author;
    String Text;
    String SearchableText;
    
    public Tweet(int tweetId, String author, String text)
    {
        TweetId = tweetId;
        Author = author;
        Text = text;
        SearchableText = Author + " " + Text;
    }
    
    public int getTweetId()
    {
        return TweetId;
    }
    
    private void setTweetId(int tweetId)
    {
        TweetId = tweetId;
    }
    
    public String getAuthor()
    {
        return Author;
    }
    
    private void setAuthor(String author)
    {
        Author = author;
    }
    
    public String getText()
    {
        return Text;
    }
    
    private void setText(String text)
    {
        Text = text;
    }
    
    public String getSearchableText()
    {
        return SearchableText;
    }
}
