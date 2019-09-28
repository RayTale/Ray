using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.DistributedTx.Abstractions;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.DistributedTx
{
    /// <summary>
    /// 分布式事务处理单元
    /// </summary>
    /// <typeparam name="Input">输出类型</typeparam>
    /// <typeparam name="Output">输出类型</typeparam>
    public abstract class DTxUnitGrain<Input, Output> : Grain, IDistributedTxUnit<Input, Output>
        where Input : class, new()
    {
        int start_id = 1;
        string start_string;
        long start_long;
        const int length = 19;
        public DTxUnitGrain()
        {
            GrainType = GetType();
            start_string = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            start_long = long.Parse(start_string);
        }
        protected Type GrainType { get; }
        private IDistributedTxStorage transactionStorage;
        protected ILogger Logger { get; private set; }
        private readonly ConcurrentDictionary<long, Commit<Input>> inputDict = new ConcurrentDictionary<long, Commit<Input>>();
        public override async Task OnActivateAsync()
        {
            Logger = (ILogger)ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(GrainType));
            transactionStorage = ServiceProvider.GetService<IDistributedTxStorage>();
            var inputList = await transactionStorage.GetList<Input>(GrainType.FullName);
            foreach (var input in inputList)
            {
                inputDict.TryAdd(input.TransactionId, input);
            }
            RegisterTimer(async state =>
            {
                foreach (var commit in inputDict.Values.ToList())
                {
                    var actors = GetTransactionActors(commit.Data);
                    try
                    {
                        await AutoCommit(commit, actors);
                    }
                    catch (Exception ex)
                    {
                        await Rollback(commit, actors);
                        Logger.LogCritical(ex, ex.Message);
                    }
                }
            }, null, new TimeSpan(0, 0, 5), new TimeSpan(0, 5, 0));
        }
        private string NewID()
        {
            var now_string = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            var now_Long = long.Parse(now_string);
            if (now_Long > start_long)
            {
                Interlocked.Exchange(ref start_string, now_string);
                Interlocked.Exchange(ref start_long, now_Long);
                Interlocked.Exchange(ref start_id, 0);
            }
            var builder = new Span<char>(new char[length]);
            var newTimes = Interlocked.Increment(ref start_id);
            if (newTimes <= 99999)
            {
                start_string.AsSpan().CopyTo(builder);

                var timesString = newTimes.ToString();
                for (int i = start_string.Length; i < length - timesString.Length; i++)
                {
                    builder[i] = '0';
                }
                var span = length - timesString.Length;
                for (int i = span; i < length; i++)
                {
                    builder[i] = timesString[i - span];
                }
                return builder.ToString();
            }
            else
            {
                return NewID();
            }
        }
        protected Task Commit(Commit<Input> commit)
        {
            return Commit(commit, GetTransactionActors(commit.Data));
        }
        private async Task Commit(Commit<Input> commit, IDistributedTx[] actors)
        {
            if (!inputDict.ContainsKey(commit.TransactionId))
            {
                commit.Status = TransactionStatus.WaitingCommit;
                await transactionStorage.Append(GrainType.FullName, commit);
                commit.Status = TransactionStatus.Persistence;
                try
                {
                    await AutoCommit(commit, actors);
                }
                catch (Exception ex)
                {
                    if (commit.Status < TransactionStatus.WaitingFinish)
                    {
                        await Rollback(commit, actors);
                        throw;
                    }
                    else
                    {
                        inputDict.TryAdd(commit.TransactionId, commit);
                    }
                    Logger.LogCritical(ex, ex.Message);
                }
            }
        }
        private async Task Rollback(Commit<Input> commit, IDistributedTx[] actors)
        {
            if (actors is null || actors.Length == 0)
                throw new NotImplementedException(nameof(GetTransactionActors));
            if (commit.Status < TransactionStatus.WaitingFinish)
            {
                await Task.WhenAll(actors.Select(a => a.RollbackTransaction(commit.TransactionId)));
                inputDict.TryRemove(commit.TransactionId, out var _);
                if (commit.Status >= TransactionStatus.Persistence)
                    await transactionStorage.Delete(GrainType.FullName, commit.TransactionId);
            }
        }
        protected Task Rollback(Commit<Input> commit)
        {
            return Rollback(commit, GetTransactionActors(commit.Data));
        }
        private async Task AutoCommit(Commit<Input> commit, IDistributedTx[] actors)
        {
            if (actors is null || actors.Length == 0)
                throw new NotImplementedException(nameof(GetTransactionActors));
            if (commit.Status == TransactionStatus.Persistence ||
                commit.Status == TransactionStatus.WaitingCommit)
            {
                await Task.WhenAll(actors.Select(a => a.CommitTransaction(commit.TransactionId)));
                await transactionStorage.Update(GrainType.FullName, commit.TransactionId, TransactionStatus.WaitingFinish);
                commit.Status = TransactionStatus.WaitingFinish;
            }
            if (commit.Status == TransactionStatus.WaitingFinish)
            {
                await Task.WhenAll(actors.Select(a => a.FinishTransaction(commit.TransactionId)));
                await transactionStorage.Delete(GrainType.FullName, commit.TransactionId);
                inputDict.TryRemove(commit.TransactionId, out var _);
                commit.Status = TransactionStatus.Finish;
            }
        }
        public async Task<Output> Ask(Input input)
        {
            var commit = new Commit<Input>
            {
                TransactionId = long.Parse(NewID()),
                Status = TransactionStatus.None,
                Data = input
            };
            return await Work(commit);
        }
        protected abstract IDistributedTx[] GetTransactionActors(Input input);
        public abstract Task<Output> Work(Commit<Input> commit);
    }
}
