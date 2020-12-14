package com.politittweet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.politittweet.Models.Tweet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class SearchEntry
{
    @FunctionName("Search")
    public HttpResponseMessage run(@HttpTrigger(name = "req", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request, final ExecutionContext context)
            throws IOException, ParseException, SQLException
    {
        DirectoryReader indexReader;
        Logger logger = context.getLogger();
        Gson gson = new Gson();
        
        logger.info("Loading application properties...");
        Properties properties = new Properties();
        properties.load(BuildIndex.class.getClassLoader()
                                .getResourceAsStream("application.properties"));
        
        Directory directory = NIOFSDirectory.open(Paths.get("").toAbsolutePath());
        
        try
        {
            indexReader = DirectoryReader.open(directory);
        }
        catch(Exception e)
        {
            ReloadIndex(context);
            indexReader = DirectoryReader.open(directory);
        }
        
        context.getLogger().info("Beginning search...");
        IndexSearcher searcher = new IndexSearcher(indexReader);
        
        // replace reader and searcher if stale
        if (DirectoryReader.openIfChanged(indexReader) != null)
        {
            indexReader.close();
            indexReader = DirectoryReader.openIfChanged(indexReader);
            searcher = new IndexSearcher(indexReader);
        }
        
        // Parse query parameter
        final String queryString = request.getBody()
                                           .orElse("");
        JsonObject jobj = new Gson().fromJson(queryString, JsonObject.class);
        String parsedQuery = jobj.get("query")
                                     .toString().replace("\"", "");
        logger.info(parsedQuery);
    
        if (parsedQuery.equals(""))
        {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                           .body("Please pass a query in the request body")
                           .build();
        }
        
        QueryParser queryParser = new QueryParser("searchText", new StandardAnalyzer());
        Query query = queryParser.parse(parsedQuery);
        TopDocs searchResults = searcher.search(query, 20);
    
        List<Tweet> results = new ArrayList<>();
        for(ScoreDoc scoreDoc: searchResults.scoreDocs)
        {
            Document resultDoc = indexReader.document(scoreDoc.doc);
            Tweet tweet = new Tweet(Integer.parseInt(resultDoc.get("id")), resultDoc.get("author"), resultDoc.get("text"));
            tweet.setScore(scoreDoc.score);
            results.add(tweet);
        }
        
        return request.createResponseBuilder(HttpStatus.OK)
                       .body(gson.toJson(results))
                       .build();
    }
    
    private HttpResponseMessage ReloadIndex(ExecutionContext context) throws IOException, SQLException
    {
        BuildIndex buildIndex = new BuildIndex();
        return buildIndex.run(null,context);
    }
}
