using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store.Azure;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Text;

namespace AzureDirectoryTests
{
    [TestClass]
    public class IntegrationTests
    {
        [TestMethod]
        public void TestReadAndWrite()
        {

            var connectionString = Environment.GetEnvironmentVariable("DataConnectionString") ?? "UseDevelopmentStorage=true";

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            // default AzureDirectory stores cache in local temp folder
            var azureDirectory = new AzureDirectory(cloudStorageAccount, "testcatalog");

            using (var indexWriter = new IndexWriter(azureDirectory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), !IndexReader.IndexExists(azureDirectory), new Lucene.Net.Index.IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH)))
            {
                indexWriter.SetRAMBufferSizeMB(10.0);

                for (int iDoc = 0; iDoc < 10000; iDoc++)
                {
                    var doc = new Document();
                    doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString() + "-" + iDoc.ToString(), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
                    doc.Add(new Field("Title", GeneratePhrase(10), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
                    doc.Add(new Field("Body", GeneratePhrase(40), Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
                    indexWriter.AddDocument(doc);
                }

                Console.WriteLine("Total docs is {0}", indexWriter.NumDocs());
            }

            using (var searcher = new IndexSearcher(azureDirectory))
            {
                Assert.AreNotEqual(0, SearchForPhrase(searcher, "dog"));
                Assert.AreNotEqual(0, SearchForPhrase(searcher, "cat"));
                Assert.AreNotEqual(0, SearchForPhrase(searcher, "car"));
            }

            // check the container exists, and delete it
            var blobClient = cloudStorageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("testcatalog");
            Assert.IsTrue(container.Exists()); // check the container exists
            container.Delete();
            
        }


        static int SearchForPhrase(IndexSearcher searcher, string phrase)
        {
            var parser = new Lucene.Net.QueryParsers.QueryParser(Lucene.Net.Util.Version.LUCENE_30, "Body", new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30));
            var query = parser.Parse(phrase);
            return searcher.Search(query, 100).TotalHits;
        }

        static Random _random = new Random();
        static string[] sampleTerms =
            {
                "dog","cat","car","horse","door","tree","chair","microsoft","apple","adobe","google","golf","linux","windows","firefox","mouse","hornet","monkey","giraffe","computer","monitor",
                "steve","fred","lili","albert","tom","shane","gerald","chris",
                "love","hate","scared","fast","slow","new","old"
            };

        private static string GeneratePhrase(int MaxTerms)
        {
            var phrase = new StringBuilder();
            int nWords = 2 + _random.Next(MaxTerms);
            for (int i = 0; i < nWords; i++)
            {
                phrase.AppendFormat(" {0} {1}", sampleTerms[_random.Next(sampleTerms.Length)], _random.Next(32768).ToString());
            }
            return phrase.ToString();
        }

    }
}
