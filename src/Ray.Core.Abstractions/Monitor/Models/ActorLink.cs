namespace Ray.Core.Abstractions.Monitor
{
    public class ActorLink
    {
        public string Actor { get; set; }
        public ActorLink Parent { get; set; }
    }
}
