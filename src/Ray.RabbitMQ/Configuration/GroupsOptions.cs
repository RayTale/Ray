namespace Ray.EventBus.RabbitMQ
{
    /// <summary>
    /// 可以针对分支进行单独配置，如果不进行配置，则使用Producer的配置
    /// </summary>
    public class GroupsOptions
    {
        public GroupOptions[] Configs { get; set; }
    }
    public class GroupOptions
    {
        public string Group { get; set; }
        public BranchOptions Config { get; set; }
    }
}
