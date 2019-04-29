using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace RushShopping.Repository
{
    public class DbContextFactory : IDesignTimeDbContextFactory<RushShoppingDbContext>
    {
        #region Implementation of IDesignTimeDbContextFactory<out RushShoppingDbContext>

        public RushShoppingDbContext CreateDbContext(string[] args)
        {
            var builder = new DbContextOptionsBuilder<RushShoppingDbContext>();
            builder.UseNpgsql(
                "Server=127.0.0.1;Port=5432;Database=rush_shopping;User Id=postgres;Password=123456;Pooling=true;Maximum Pool Size=20;");
            return new RushShoppingDbContext(builder.Options);
        }

        #endregion
    }
}