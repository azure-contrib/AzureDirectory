using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Store;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Directory = Lucene.Net.Store.Directory;

namespace AzureDirectory.Core {
    public class AzureDirectory : Directory {
        private readonly string _containerName;
        private readonly string _rootFolder;
        private CloudBlobClient _blobClient;
        private readonly Dictionary<string, AzureIndexOutput> _nameCache = new Dictionary<string, AzureIndexOutput>();
        private readonly Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();
        private LockFactory _lockFactory = new NativeFSLockFactory();
        public override LockFactory LockFactory => _lockFactory;
        public CloudBlobContainer BlobContainer { get; private set; }

        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="containerName">name of container (folder in blob storage)</param>
        /// <param name="rootFolder">path of the root folder inside the container</param>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string containerName = null,
            string rootFolder = null) {

            _rootFolder = rootFolder;
            if (storageAccount == null)
                throw new ArgumentNullException(nameof(storageAccount));

            _containerName = string.IsNullOrEmpty(containerName) ? "lucene" : containerName.ToLower();


            if (string.IsNullOrEmpty(rootFolder))
                _rootFolder = string.Empty;
            else {
                rootFolder = rootFolder.Trim('/');
                _rootFolder = rootFolder + "/";
            }


            _blobClient = storageAccount.CreateCloudBlobClient();
            CreateContainer();
        }

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override string[] ListAll() {
            var results = from blob in BlobContainer.ListBlobs(_rootFolder)
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);
            return results.ToArray();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        [Obsolete("this method will be removed in 5.0")]
        public override bool FileExists(string name) {
            // this always comes from the server
            try {
                return BlobContainer.GetBlockBlobReference(_rootFolder + name).Exists();
            }
            catch (Exception) {
                return false;
            }
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name) {
            var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
            blob.DeleteIfExists();
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name) {
            var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
            blob.FetchAttributes();

            // index files may be compressed so the actual length is stored in metadata
            var hasMetadataValue = blob.Metadata.TryGetValue("CachedLength", out var blobLegthMetadata);

            if (hasMetadataValue && long.TryParse(blobLegthMetadata, out var blobLength)) {
                return blobLength;
            }
            return blob.Properties.Length; // fall back to actual blob size
        }

        public override void Sync(ICollection<string> names) {
            // TODO: This all is purely guesswork, no idea what has to be done here. -- Aviad.
            foreach (var name in names) {
                if (_nameCache.ContainsKey(name)) {
                    _nameCache[name].Flush();
                }
            }
        }

        public override IndexInput OpenInput(string name, IOContext context) {
            // TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
            try {
                var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
                blob.FetchAttributes();
                return new AzureIndexInput(name, blob);
            }
            catch (Exception err) {
                throw new FileNotFoundException(name, err);
            }
        }

        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock MakeLock(string name) {
            lock (_locks) {
                if (!_locks.ContainsKey(name)) {
                    _locks.Add(name, new AzureLock(_rootFolder + name, this));
                }
                return _locks[name];
            }
        }

        public override void ClearLock(string name) {
            lock (_locks) {
                if (_locks.ContainsKey(name)) {
                    _locks[name].BreakLock();
                }
            }
        }

        /// <summary>Closes the store. </summary>
        protected override void Dispose(bool disposing) {
            BlobContainer = null;
            _blobClient = null;
        }

        public override void SetLockFactory(LockFactory lockFactory) {
            _lockFactory = lockFactory;
        }

        public void CreateContainer() {
            BlobContainer = _blobClient.GetContainerReference(_containerName);
            BlobContainer.CreateIfNotExists();
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(string name, IOContext context) {
            // TODO: Figure out how IOContext comes into play here. So far it doesn't -- Aviad
            var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
            var indexOutput = new AzureIndexOutput(blob);
            _nameCache[name] = indexOutput;
            return indexOutput;
        }
    }
}
