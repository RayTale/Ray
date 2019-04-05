using System;
using Microsoft.EntityFrameworkCore;
namespace RushShopping.Repository
{
    public class RushShoppingDbContext : DbContext
    {
        public RushShoppingDbContext(DbContextOptions<RushShoppingDbContext> dbContextOptions) : base(dbContextOptions)
        {
        }
    }
}
