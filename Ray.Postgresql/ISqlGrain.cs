namespace Ray.Postgresql
{
    public interface ISqlGrain
    {
        SqlGrainConfig ESSQLTable { get; }
    }
}
