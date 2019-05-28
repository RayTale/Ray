using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ray.DistributedTransaction
{
    public abstract class DistributedTxUnit<Input, Output> : Grain, IDistributedTxUnit<Input, Output>
    {
        public DistributedTxUnit(ILogger logger)
        {
            Logger = logger;
        }
        private Type GrainType;
        private IDistributedTxStorage transactionStorage;
        protected ILogger Logger { get; private set; }
        private readonly ConcurrentDictionary<long, Commit<Input>> inputDict = new ConcurrentDictionary<long, Commit<Input>>();
        public override async Task OnActivateAsync()
        {
            GrainType = GetType();
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
            }, null, new TimeSpan(0, 5, 0), new TimeSpan(0, 1, 0));
        }
        int newStringByUtcTimes = 1;
        long newStringByUtcStart = long.Parse(DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));
        protected async ValueTask<string> NewTransactionId()
        {
            var nowTimestamp = long.Parse(DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));
            if (nowTimestamp > newStringByUtcStart)
            {
                Interlocked.Exchange(ref newStringByUtcStart, nowTimestamp);
                Interlocked.Exchange(ref newStringByUtcTimes, 0);
            }
            var utcBuilder = new StringBuilder(22);
            var newTimes = Interlocked.Increment(ref newStringByUtcTimes);
            if (newTimes <= 999999)
            {
                utcBuilder.Clear();
                utcBuilder.Append(newStringByUtcStart.ToString());
                var timesString = newTimes.ToString();
                for (int i = 0; i < 4 - timesString.Length; i++)
                {
                    utcBuilder.Append("0");
                }
                utcBuilder.Append(timesString);
                return utcBuilder.ToString();
            }
            else
            {
                await Task.Delay(1000);
                var task = NewTransactionId();
                if (!task.IsCompletedSuccessfully)
                    await task;
                return task.Result;
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
            if (actors == default || actors.Length == 0)
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
            if (actors == default || actors.Length == 0)
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
            var newIdTask = NewTransactionId();
            if (!newIdTask.IsCompletedSuccessfully)
                await newIdTask;
            var commit = new Commit<Input>
            {
                TransactionId = long.Parse(newIdTask.Result),
                Status = TransactionStatus.None,
                Data = input
            };
            return await Work(commit);
        }
        public abstract IDistributedTx[] GetTransactionActors(Input input);
        public abstract Task<Output> Work(Commit<Input> commit);
    }
}
