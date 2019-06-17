using System;
using System.Diagnostics;
using System.Threading;
using JetBrains.Annotations;
using Lucene.Net.Store;
using Microsoft.Azure.Storage.Blob;

namespace AzureDirectory.Core {
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput {
        private AzureDirectory _azureDirectory;
        private CloudBlobContainer _blobContainer;
        private ICloudBlob _blob;
        private readonly string _name;

        private IndexInput _indexInput;
        private readonly Mutex _fileMutex;

        public Directory CacheDirectory => _azureDirectory.CacheDirectory;

        [UsedImplicitly]
        public AzureIndexInput(string resourceDescription, AzureDirectory azuredirectory, ICloudBlob blob,
                               IOContext context)
            : base(resourceDescription) {
            _name = blob.Uri.Segments[^1];

#if FULLDEBUG
            Debug.WriteLine(String.Format("opening {0} ", _name));
#endif
            _fileMutex = BlobMutexManager.GrabMutex(_name);
            _fileMutex.WaitOne();
            try {
                _azureDirectory = azuredirectory;
                _blobContainer = azuredirectory.BlobContainer;
                _blob = blob;

                var fileName = _name;

                // open the file in read only mode
                _indexInput = CacheDirectory.OpenInput(fileName, context);
            }
            finally {
                _fileMutex.ReleaseMutex();
            }
        }

        public AzureIndexInput(string resourceDescription, AzureIndexInput cloneInput) : base(resourceDescription) {
            _fileMutex = BlobMutexManager.GrabMutex(cloneInput._name);
            _fileMutex.WaitOne();

            try {
#if FULLDEBUG
                Debug.WriteLine(String.Format("Creating clone for {0}", cloneInput._name));
#endif
                _azureDirectory = cloneInput._azureDirectory;
                _blobContainer = cloneInput._blobContainer;
                _blob = cloneInput._blob;
                _indexInput = cloneInput._indexInput.Clone() as IndexInput;
            }
            catch (Exception) {
                // sometimes we get access denied on the 2nd stream...but not always. I haven't tracked it down yet
                // but this covers our tail until I do
                Debug.WriteLine($"Dagnabbit, falling back to memory clone for {cloneInput._name}");
            }
            finally {
                _fileMutex.ReleaseMutex();
            }
        }

        public override byte ReadByte() {
            return _indexInput.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len) {
            _indexInput.ReadBytes(b, offset, len);
        }

        public override long GetFilePointer() {
            return _indexInput.GetFilePointer();
        }

        public override void Seek(long pos) {
            _indexInput.Seek(pos);
        }

        protected override void Dispose(bool disposing) {
            _fileMutex.WaitOne();
            try {
#if FULLDEBUG
                Debug.WriteLine(String.Format("CLOSED READSTREAM local {0}", _name));
#endif
                _indexInput.Dispose();
                _indexInput = null;
                _azureDirectory = null;
                _blobContainer = null;
                _blob = null;
                GC.SuppressFinalize(this);
            }
            finally {
                _fileMutex.ReleaseMutex();
            }
        }

        public override object Clone() {
            IndexInput clone = null;
            try {
                _fileMutex.WaitOne();
                var input = new AzureIndexInput(base.ToString(), this);
                clone = input;
            }
            catch (Exception err) {
                Debug.WriteLine(err.ToString());
            }
            finally {
                _fileMutex.ReleaseMutex();
            }
            Debug.Assert(clone != null);
            return clone;
        }

        public override long Length => _indexInput.Length;
    }
}
