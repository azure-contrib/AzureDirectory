using System.IO;
using Lucene.Net.Store;
using Microsoft.Azure.Storage.Blob;

namespace AzureDirectory.Core {
    /// <summary>
    /// Implements IndexInput semantics for a read only blob
    /// </summary>
    public class AzureIndexInput : IndexInput {
        private readonly string _sometoken;
        private readonly Stream _stream;

        public AzureIndexInput(string sometoken, CloudBlob blob)
            : base(sometoken) {
            _sometoken = sometoken;
            _stream = blob.OpenRead();
        }

        public override byte ReadByte() {
            return (byte)_stream.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len) {
            _stream.Read(b, offset, len);
        }

        protected override void Dispose(bool disposing) {
        }

        public override long GetFilePointer() {
            return _stream.Position;
        }

        public override void Seek(long pos) {
            _stream.Seek(pos, SeekOrigin.Begin);
        }

        public override long Length => _stream.Length;
    }
}
