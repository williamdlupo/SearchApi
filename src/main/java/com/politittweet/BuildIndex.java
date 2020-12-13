package com.politittweet;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.politittweet.Models.Tweet;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

public class BuildIndex
{
    @FunctionName("BuildIndex")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET},
                    authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws SQLException, IOException
    {
        Logger logger = context.getLogger();
        String tweetEntityQuery = "select t.id, e.name, t.text from Tweet t, Entity e where t.entityId = e.id";
    
        logger.info("Loading application properties...");
        Properties properties = new Properties();
        properties.load(BuildIndex.class.getClassLoader().getResourceAsStream("application.properties"));
        
        Directory directory = FSDirectory.open(Paths.get("").toAbsolutePath());
        IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig());
        
        // connect to DB
        logger.info("Connecting to the database...");
        Connection connection = DriverManager.getConnection(properties.getProperty("connectionString"), properties);
        
        // get all content to index - DB id, author and text
        PreparedStatement readStatement = connection.prepareStatement(tweetEntityQuery);
        ResultSet resultSet = readStatement.executeQuery();
        
        if (!resultSet.next()) {
            logger.info("There is no data in the database!");
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Unable to query database").build();
        }
    
        logger.info("Retrieving database results...");
        while(resultSet.next())
        {
            Tweet tweet  = new Tweet(resultSet.getInt("id"), resultSet.getString("name"), resultSet.getString("text"));
    
            Document document = new Document();
            document.add(new StoredField ("id", tweet.getTweetId()));
            document.add(new TextField("author", tweet.getAuthor(), Field.Store.YES));
            document.add(new TextField("searchText", tweet.getSearchableText(), Field.Store.YES));
            indexWriter.addDocument(document);
        }
        connection.close();
        logger.info("Content stored to index writer.");
        
        // create/overwrite index file and store to file system
        indexWriter.commit();
        indexWriter.close();
        logger.info("Index successfully updated!");
    
        return request.createResponseBuilder(HttpStatus.OK).body("Index successfully updated").build();
    }
}