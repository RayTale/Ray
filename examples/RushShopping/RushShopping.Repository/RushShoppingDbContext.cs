using Microsoft.EntityFrameworkCore;
using RushShopping.Repository.Entities;

namespace RushShopping.Repository
{
    public class RushShoppingDbContext : DbContext
    {
        public RushShoppingDbContext(DbContextOptions<RushShoppingDbContext> contextOptions) : base(contextOptions)
        {

        }

        public DbSet<Customer> Customers { get; set; }

        public DbSet<Product> Products { get; set; }

        public DbSet<ProductOrder> ProductOrders { get; set; }
    }
}