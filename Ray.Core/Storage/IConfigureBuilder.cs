namespace Ray.Core.Storage
{
    public interface IConfigureBuilder<PrimaryKey, Config, Parameter>
    {
        IConfigureBuilder<PrimaryKey, Config, Parameter> AllotTo<Grain>(Parameter parameter);
        void Complete(IConfigureBuilderContainer container = default);
    }
}
