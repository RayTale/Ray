using System;

namespace Ray.Core.Exceptions
{
    public class ChannelUnavailabilityException : Exception
    {
        public ChannelUnavailabilityException(string id, Type grainType) : base($"Channel unavailability,type {grainType.FullName} with id {id}")
        {
        }
    }
}
