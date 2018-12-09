using ProtoBuf;
using System;

namespace RayTest.IGrains.Model
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class Transfer
    {
        public string Id { get; set; }
        public decimal Amount { get; set; }
        public int To { get; set; }
        public DateTime CreateTime { get; set; }
    }
}
