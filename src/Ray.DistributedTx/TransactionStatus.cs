namespace Ray.DistributedTx
{
    /// <summary>
    /// 事务状态
    /// </summary>
    public enum TransactionStatus
    {
        None = 0,
        Persistence = 1,
        WaitingCommit = 2,
        WaitingFinish = 3,
        Finish = 4
    }
}
