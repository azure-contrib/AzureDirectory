using System;
using System.IO;

namespace Lucene.Net.Store.Azure
{
  /// <summary>
  /// Stream wrapper around an IndexOutput
  /// </summary>
  public class StreamOutput : Stream
  {
    protected IndexOutput Output { get; set; }

    public StreamOutput(IndexOutput output)
    {
      this.Output = output;
    }

    public override bool CanRead => false;

    public override bool CanSeek => true;

    public override bool CanWrite => true;

    public override void Flush() => this.Output.Flush();

    public override long Length => this.Output.Length;

    public override long Position
    {
      get => this.Output.GetFilePointer();
      set => this.Output.Seek(value);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
      throw new NotImplementedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
      switch (origin)
      {
        case SeekOrigin.Begin:
          this.Output.Seek(offset);
          break;
        case SeekOrigin.Current:
          this.Output.Seek(Output.GetFilePointer() + offset);
          break;
        case SeekOrigin.End:
          throw new NotImplementedException();
      }
      return Output.GetFilePointer();
    }

    public override void SetLength(long value) => throw new NotImplementedException();

    public override void Write(byte[] buffer, int offset, int count) => this.Output.WriteBytes(buffer, offset, count);

    public override void Close()
    {
      Output.Flush();
      Output.Dispose();
      base.Close();
    }
  }
}
