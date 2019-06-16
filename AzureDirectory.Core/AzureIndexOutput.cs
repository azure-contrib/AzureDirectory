using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;
using Lucene.Net.Store;
using Microsoft.Azure.Storage.Blob;

namespace AzureDirectory.Core
{
    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class AzureIndexOutput : IndexOutput
    {
        private readonly AzureDirectory _azureDirectory;
        private readonly string _name;
        private IndexOutput _indexOutput;
        private readonly Mutex _fileMutex;
        private ICloudBlob _blob;
        public Lucene.Net.Store.Directory CacheDirectory => _azureDirectory.CacheDirectory;

        public AzureIndexOutput(AzureDirectory azureDirectory, ICloudBlob blob)
        {
            _fileMutex = BlobMutexManager.GrabMutex(_name); 
            _fileMutex.WaitOne();
            try
            {
                _azureDirectory = azureDirectory;
                _blob = blob;
                _name = blob.Uri.Segments[^1];

                // create the local cache one we will operate against...
                _indexOutput = CacheDirectory.CreateOutput(_name, new IOContext(IOContext.UsageContext.DEFAULT));
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        public override void Flush()
        {
            _indexOutput.Flush();
        }

        protected override void Dispose(bool disposing)
        {
            _fileMutex.WaitOne();
            try
            {
                var fileName = _name;

                // make sure it's all written out
                _indexOutput.Flush();

                var originalLength = _indexOutput.Length;
                _indexOutput.Dispose();

                Stream blobStream;

                // optionally put a compressor around the blob stream
                if (_azureDirectory.ShouldCompressFile(_name))
                {
                    blobStream = CompressStream(fileName, originalLength);
                }
                else
                {
                    blobStream = new StreamInput(CacheDirectory.OpenInput(fileName, new IOContext()));
                }

                try
                {
                    // push the blobStream up to the cloud
                    _blob.UploadFromStream(blobStream);

                    // set the metadata with the original index file properties
                    _blob.Metadata["CachedLength"] = originalLength.ToString();
                    _blob.SetMetadata();

                    Debug.WriteLine("PUT {1} bytes to {0} in cloud", _name, blobStream.Length);
                }
                finally
                {
                    blobStream.Dispose();
                }

#if FULLDEBUG
                Debug.WriteLine(string.Format("CLOSED WRITESTREAM {0}", _name));
#endif
                // clean up
                _indexOutput = null;
                _blob = null;
                GC.SuppressFinalize(this);
            }
            finally
            {
                _fileMutex.ReleaseMutex();
            }
        }

        private MemoryStream CompressStream(string fileName, long originalLength)
        {
            // unfortunately, deflate stream doesn't allow seek, and we need a seekable stream
            // to pass to the blob storage stuff, so we compress into a memory stream
            var compressedStream = new MemoryStream();

            try
            {
                using (var indexInput = CacheDirectory.OpenInput(fileName, new IOContext()))
                using (var compressor = new DeflateStream(compressedStream, CompressionMode.Compress, true))
                {
                    // compress to compressedOutputStream
                    var bytes = new byte[indexInput.Length];
                    indexInput.ReadBytes(bytes, 0, bytes.Length);
                    compressor.Write(bytes, 0, bytes.Length);
                }

                // seek back to beginning of compressed stream
                compressedStream.Seek(0, SeekOrigin.Begin);

                Debug.WriteLine(
                    $"COMPRESSED {originalLength} -> {compressedStream.Length} {((float) compressedStream.Length / (float) originalLength) * 100}% to {_name}");
            }
            catch
            {
                // release the compressed stream resources if an error occurs
                compressedStream.Dispose();
                throw;
            }
            return compressedStream;
        }

        public override long Length => _indexOutput.Length;

        public override void WriteByte(byte b)
        {
            _indexOutput.WriteByte(b);
        }

        public override void WriteBytes(byte[] b, int length)
        {
            _indexOutput.WriteBytes(b, length);
        }

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            _indexOutput.WriteBytes(b, offset, length);
        }

        public override long GetFilePointer() => _indexOutput.GetFilePointer();

        [Obsolete("(4.1) this method will be removed in Lucene 5.0")]
        public override void Seek(long pos)
        {
            _indexOutput.Seek(pos);
        }

        public override long Checksum => _indexOutput.Checksum;
    }
}
