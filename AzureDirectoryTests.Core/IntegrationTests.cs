using System;
using System.Text;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AzureDirectoryTests.Core {
    [TestClass]
    public class IntegrationTests {
        private readonly string _connectionString;

        public IntegrationTests(string connectionString = null) {
            _connectionString = connectionString;
        }

        [TestMethod]
        public void TestReadAndWrite() {

            var connectionString = _connectionString ?? "UseDevelopmentStorage=true";

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            var azureDirectory = new AzureDirectory.Core.AzureDirectory(cloudStorageAccount, "testcatalog");

            using (var indexWriter = new IndexWriter(azureDirectory,
                                                     new IndexWriterConfig(
                                                         Lucene.Net.Util.LuceneVersion.LUCENE_48,
                                                         new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48)))
            ) {

                for (var iDoc = 0; iDoc < 10000; iDoc++) {
                    var doc = new Document {
                        new StringField("id", DateTime.Now.ToFileTimeUtc() + "-" + iDoc, Field.Store.YES),
                        new StringField("Title", GeneratePhrase(10), Field.Store.YES),
                        new StringField("Body", GeneratePhrase(40), Field.Store.YES)
                    };
                    indexWriter.AddDocument(doc);
                }

                Console.WriteLine("Total docs is {0}", indexWriter.NumDocs);
            }

            for (var i = 0; i < 100; i++) {
                var ireader = DirectoryReader.Open(azureDirectory);
                var searcher = new IndexSearcher(ireader);
                var searchForPhrase = SearchForPhrase(searcher, "dog");
                Assert.AreNotEqual(0, searchForPhrase);
                Assert.AreNotEqual(0, SearchForPhrase(searcher, "cat"));
                Assert.AreNotEqual(0, SearchForPhrase(searcher, "car"));
            }

            // check the container exists, and delete it
            var blobClient = cloudStorageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("testcatalog");
            Assert.IsTrue(container.Exists()); // check the container exists
            container.Delete();

        }


        private static int SearchForPhrase(IndexSearcher searcher, string phrase) {
            var parser = new Lucene.Net.QueryParsers.Classic.QueryParser(Lucene.Net.Util.LuceneVersion.LUCENE_48, "Body", new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48));
            var query = parser.Parse(phrase);
            return searcher.Search(query, 100).TotalHits;
        }

        private static readonly Random Random = new Random();

        private static readonly string[] SampleTerms = {
            "dog", "cat", "car", "horse", "door", "tree", "chair", "microsoft", "apple", "adobe", "google", "golf",
            "linux", "windows", "firefox", "mouse", "hornet", "monkey", "giraffe", "computer", "monitor",
            "steve", "fred", "lili", "albert", "tom", "shane", "gerald", "chris",
            "love", "hate", "scared", "fast", "slow", "new", "old"
        };

        private static string GeneratePhrase(int maxTerms) {
            var phrase = new StringBuilder();
            var nWords = 2 + Random.Next(maxTerms);
            for (var i = 0; i < nWords; i++) {
                phrase.AppendFormat(" {0} {1}", SampleTerms[Random.Next(SampleTerms.Length)],
                                    Random.Next(32768).ToString());
            }
            return phrase.ToString();
        }

    }
}
