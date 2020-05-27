using Ray.Core.Abstractions;
using Ray.Core.Serialization;
using Ray.Core.Utils;
using System;
using System.Text;

namespace Ray.Core.Event
{
    public class EventBox<PrimaryKey> : IDisposable
    {
        private string _TypeCode;
        private byte[] _EventBytes;
        private SharedArray _EventBaseArray;
        private SharedArray _Array;
        public EventBox(FullyEvent<PrimaryKey> fullyEvent, EventUID eventUID, string uniqueId, string hashKey)
        {
            FullyEvent = fullyEvent;
            EventUID = eventUID;
            UniqueId = uniqueId;
            HashKey = hashKey;

        }
        public FullyEvent<PrimaryKey> FullyEvent { get; }
        public EventUID EventUID { get; set; }
        public string UniqueId { get; set; }
        public string HashKey { get; set; }
        public string EventUtf8String { get; private set; }
        public void Parse(ITypeFinder typeFinder, ISerializer serializer)
        {
            if (EventUtf8String == default)
            {
                var evtType = FullyEvent.Event.GetType();
                _TypeCode = typeFinder.GetCode(evtType);
                _EventBaseArray = FullyEvent.BasicInfo.ConvertToBytes();
                _EventBytes = serializer.SerializeToUtf8Bytes(FullyEvent.Event, evtType);
                _Array = GetConverter().ConvertToBytes();
                EventUtf8String = Encoding.UTF8.GetString(_EventBytes);
            }
        }
        public EventConverter GetConverter()
        {
            return new EventConverter(_TypeCode, FullyEvent.ActorId, _EventBaseArray.AsSpan(), _EventBytes);
        }
        public Span<byte> GetSpan() => _Array.AsSpan();
        public void Dispose()
        {
            _EventBaseArray.Dispose();
            _Array.Dispose();
        }
    }
}
