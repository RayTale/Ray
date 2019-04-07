﻿using ProtoBuf;
using System;

namespace Ray.IGrains.Model
{
    [ProtoContract(ImplicitFields = ImplicitFields.AllFields)]
    public class Transfer
    {
        public string Id { get; set; }
        public decimal Amount { get; set; }
        public int To { get; set; }
        public long CreateTime { get; set; }
    }
}
