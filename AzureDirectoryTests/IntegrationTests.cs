using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store.Azure;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Text;

namespace AzureDirectoryTests
{
    [TestClass]
    public class IntegrationTests
    {
        private readonly string _connectionString;

        public IntegrationTests() : this(null) { }

        public IntegrationTests(string connectionString)
        {
            _connectionString = connectionString;
        }
        [TestMethod]
        public void TestReadAndWrite()
        {

            var connectionString = _connectionString ?? "UseDevelopmentStorage=true";

            var cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            const string containerName = "testcatalog2";

            var azureDirectory = new AzureDirectory(cloudStorageAccount,@"d:\LuceneTests", containerName:containerName);

            var indexWriterConfig = new IndexWriterConfig(
                Lucene.Net.Util.LuceneVersion.LUCENE_48,
                new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48));

            int dog = 0, cat = 0, car = 0;

            using (var indexWriter = new IndexWriter(azureDirectory, indexWriterConfig))
            {

                for (var iDoc = 0; iDoc < 10000; iDoc++)
                {
                    var bodyText = GeneratePhrase(40);
                    var doc = new Document {
                        new TextField("id", DateTime.Now.ToFileTimeUtc() + "-" + iDoc, Field.Store.YES),
                        new TextField("Title", GeneratePhrase(10), Field.Store.YES),
                        new TextField("Body", bodyText, Field.Store.YES)
                    };
                    dog += bodyText.Contains(" dog ") ? 1 : 0;
                    cat += bodyText.Contains(" cat ") ? 1 : 0;
                    car += bodyText.Contains(" car ") ? 1 : 0;
                    indexWriter.AddDocument(doc);
                }

                Console.WriteLine("Total docs is {0}, {1} dog, {2} cat, {3} car", indexWriter.NumDocs, dog, cat, car);
            }
            try
            {

                var ireader = DirectoryReader.Open(azureDirectory);
                for (var i = 0; i < 100; i++)
                {
                    var searcher = new IndexSearcher(ireader);
                    var searchForPhrase = SearchForPhrase(searcher, "dog");
                    Assert.AreEqual(dog, searchForPhrase);
                    searchForPhrase = SearchForPhrase(searcher, "cat");
                    Assert.AreEqual(cat, searchForPhrase);
                    searchForPhrase = SearchForPhrase(searcher, "car");
                    Assert.AreEqual(car, searchForPhrase);
                }
                Console.WriteLine("Tests passsed");
            }
            catch (Exception x)
            {
                Console.WriteLine("Tests failed:\n{0}", x);
            }
            finally
            {
                // check the container exists, and delete it
                var blobClient = cloudStorageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(containerName);
                Assert.IsTrue(container.Exists()); // check the container exists
                container.Delete();
            }
        }

        private static int SearchForPhrase(IndexSearcher searcher, string phrase)
        {
            var parser = new Lucene.Net.QueryParsers.Classic.QueryParser(Lucene.Net.Util.LuceneVersion.LUCENE_48, "Body", new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48));
            var query = parser.Parse(phrase);
            var topDocs = searcher.Search(query, 100);
            return topDocs.TotalHits;
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
