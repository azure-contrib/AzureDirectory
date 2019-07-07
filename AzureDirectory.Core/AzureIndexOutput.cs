using System;
using System.Linq;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Microsoft.Azure.Storage.Blob;

namespace AzureDirectory.Core {
    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class AzureIndexOutput : IndexOutput {
        private readonly CloudBlobStream _stream;
        private readonly CRC32 _crc;

        public AzureIndexOutput(CloudBlockBlob blob) {
            _stream = blob.OpenWrite();
            _crc = new CRC32();
        }

        public override void WriteByte(byte b) {
            _stream.WriteByte(b);
            _crc.Update(b);
        }

        public override void WriteBytes(byte[] b, int offset, int length) {
            _stream.Write(b, offset, length);
            _crc.Update(b, offset, length);
        }

        public override void Flush() {
            try {
                _stream.Flush();
            }
            catch {
                // ignored
            }
        }

        protected override void Dispose(bool disposing) {
            _stream?.Dispose();
        }

        public override long GetFilePointer() {
            return _stream.Position;
        }

        [Obsolete]
        public override void Seek(long pos) {
            throw new NotSupportedException();
        }

        public override long Checksum => _crc.Value;
    }
}
