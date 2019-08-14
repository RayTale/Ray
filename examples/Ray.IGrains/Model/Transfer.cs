namespace Ray.IGrains.Model
{
    public class Transfer
    {
        public string Id { get; set; }
        public decimal Amount { get; set; }
        public int To { get; set; }
        public long CreateTime { get; set; }
    }
}
