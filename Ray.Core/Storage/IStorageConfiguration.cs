using System.Threading.Tasks;

namespace Ray.Core.Storage
{
    public interface IStorageConfiguration<Config, Parameter>
    {
        Task Configure(IConfigureBuilderContainer container);
    }
}
