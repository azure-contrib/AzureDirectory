using System;
using Microsoft.Extensions.Configuration;

namespace Test {
    class Program {
        private static IConfigurationRoot Configuration;
        private const string ConnectionSecretName = "AzureStorage:ConnectionString";

        static void Main(string[] args) {
            var builder = new ConfigurationBuilder();

            builder.AddUserSecrets<Program>();

            Configuration = builder.Build();

            var connectionString = Configuration[ConnectionSecretName];

            AzureDirectoryTests.Core.IntegrationTests tx = new AzureDirectoryTests.Core.IntegrationTests(connectionString);
            tx.TestReadAndWrite();
        }
    }
}
