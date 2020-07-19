using System;
using System.Text;
using Ray.Core.Abstractions;
using Ray.Core.Serialization;
using Ray.Core.Utils;

namespace Ray.Core.Event
{
    public class EventBox<PrimaryKey> : IDisposable
    {
        private string TypeCode;
        private byte[] EventBytes;
        private SharedArray EventBaseArray;
        private SharedArray Array;

        public EventBox(FullyEvent<PrimaryKey> fullyEvent, EventUID eventUID, string uniqueId, string hashKey)
        {
            this.FullyEvent = fullyEvent;
            this.EventUID = eventUID;
            this.UniqueId = uniqueId;
            this.HashKey = hashKey;

        }

        public FullyEvent<PrimaryKey> FullyEvent { get; }

        public EventUID EventUID { get; set; }

        public string UniqueId { get; set; }

        public string HashKey { get; set; }

        public string EventUtf8String { get; private set; }

        public void Parse(ITypeFinder typeFinder, ISerializer serializer)
        {
            if (this.EventUtf8String == default)
            {
                var evtType = this.FullyEvent.Event.GetType();
                this.TypeCode = typeFinder.GetCode(evtType);
                this.EventBaseArray = this.FullyEvent.BasicInfo.ConvertToBytes();
                this.EventBytes = serializer.SerializeToUtf8Bytes(this.FullyEvent.Event, evtType);
                this.Array = this.GetConverter().ConvertToBytes();
                this.EventUtf8String = Encoding.UTF8.GetString(this.EventBytes);
            }
        }

        public EventConverter GetConverter()
        {
            return new EventConverter(this.TypeCode, this.FullyEvent.StateId, this.EventBaseArray.AsSpan(), this.EventBytes);
        }

        public Span<byte> GetSpan() => this.Array.AsSpan();

        public void Dispose()
        {
            this.EventBaseArray.Dispose();
            this.Array.Dispose();
        }
    }
}
