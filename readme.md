# AzureDirectory Library for Lucene.Net

A fork of this project: https://azuredirectory.codeplex.com/
Updated to work with Lucene 3.0.3, and version 2.1.0.3 of the Azure Storage client.

## Project description

Lucene.Net is a robust open source search technology which has an abstract interface called a Directory for defining how the index is stored. AzureDirectory is an implementation of that interface for Windows Azure Blob Storage.

## About
This project allows you to create Lucene Indexes and use them in Azure.

This project implements a low level Lucene Directory object called AzureDirectory around Windows Azure BlobStorage.

## Background
Lucene is a mature Java based open source full text indexing and search engine and property store. 
Lucene.NET is a mature port of that library to C#.
Lucene/Lucene.Net provides:
* Super simple API for storing documents with arbitrary properties
* Complete control over what is indexed and what is stored for retrieval
* Robust control over where and how things are indexed, how much memory is used, etc.
* Superfast and super rich query capabilities
	* Sorted results
	* Rich constraint semantics AND/OR/NOT etc.
	* Rich text semantics (phrase match, wildcard match, near, fuzzy match etc)
	* Text query syntax (example: Title:(dog AND cat) OR Body:Lucen* )
	* Programmatic expressions
	* Ranked results with custom ranking algorithms
 
## AzureDirectory
AzureDirectory smartly uses a local Directory to cache files as they are created and automatically pushes them to Azure blob storage as appropriate. Likewise, it smartly caches blob files on the client when they change. This provides with a nice blend of just in time syncing of data local to indexers or searchers across multiple machines.

With the flexibility that Lucene provides over data in memory versus storage and the just in time blob transfer that AzureDirectory provides you have great control over the composibility of where data is indexed and how it is consumed.

To be more concrete: you can have 1..N worker roles adding documents to an index, and 1..N searcher webroles searching over the catalog in near real time.

## Usage

To use you need to create a blob storage account on http://azure.com .

Create an App.Config or Web.Config and configure your accountinfo:

```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
	<appSettings>
		<!-- azure SETTINGS -->
		<add key="BlobStorageEndpoint" value="http://YOURACCOUNT.blob.core.windows.net"/>
		<add key="AccountName" value="YOURACCOUNTNAME"/>
		<add key="AccountSharedKey" value="YOURACCOUNTKEY"/>
	</appSettings>
</configuration>
```         
 
To add documents to a catalog is as simple as

```cs
var azureDirectory = new AzureDirectory("TestCatalog");
var indexWriter = new IndexWriter(azureDirectory, new StandardAnalyzer(), true);
var doc = new Document();

doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
doc.Add(new Field("Title", "this is my title", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));
doc.Add(new Field("Body", "This is my body", Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.NO));

indexWriter.AddDocument(doc);
indexWriter.Close();
```
 

And searching is as easy as:

```cs
var searcher = new IndexSearcher(azureDirectory);               
var parser = QueryParser("Title", new StandardAnalyzer());
var query = parser.Parse("Title:(Dog AND Cat)");

var hits = searcher.Search(query);
for (int i = 0; i < hits.Length(); i++)
{
    var doc = hits.Doc(i);
    Console.WriteLine(doc.GetField("Title").StringValue());
}
```            
 
 
## Caching and Compression

AzureDirectory compresses blobs before sent to the blob storage. Blobs are automatically cached local to reduce roundtrips for blobs which haven't changed. 

By default AzureDirectory stores this local cache in a temporary folder. You can easily control where the local cache is stored by passing in a Directory object for whatever type and location of storage you want.

This example stores the cache in a ram directory:

```cs
var azureDirectory = new AzureDirectory("MyIndex", new RAMDirectory());
```
 
And this example stores in the file system in C:\myindex

```cs
var azureDirectory = new AzureDirectory("MyIndex", new FSDirectory(@"c:\myindex"));
```

## Notes on settings

Just like a normal Lucene index, calling optimize too often causes a lot of churn and not calling it enough causes too many segment files to be created, so call it "just enough" times. That will totally depend on your application and the nature of your pattern of adding and updating items to determine (which is why Lucene provides so many knobs to configure its behavior).

The default compound file support that Lucene uses reduces the number of files that are generated...this means it deletes and merges files regularly which causes churn on the blob storage. Calling indexWriter.SetCompoundFiles(false) will give better performance. 

The version of Lucene.NET checked in as a binary is Version 3.0.3, but you can use any version of Lucene.NET you want by simply enlisting from the above open source site.

## FAQ

There is a LINQ to Lucene provider http://linqtoLucene.codeplex.com/Wiki/View.aspx?title=Project%20Documentation  on codeplex which allows you to define your schema as a strongly typed object and execute LINQ expressions against the index.

## License

MIT