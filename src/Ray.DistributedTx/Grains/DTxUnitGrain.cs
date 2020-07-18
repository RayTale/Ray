using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Ray.DistributedTx.Abstractions;

namespace Ray.DistributedTx
{
    /// <summary>
    /// 分布式事务处理单元
    /// </summary>
    /// <typeparam name="TInput">输出类型</typeparam>
    /// <typeparam name="TOutput">输出类型</typeparam>
    public abstract class DTxUnitGrain<TInput, TOutput> : Grain, IDistributedTxUnit<TInput, TOutput>
        where TInput : class, new()
    {
        private int startId = 1;
        private string startString;
        private long startLong;
        private const int Length = 19;

        public DTxUnitGrain()
        {
            this.GrainType = this.GetType();
            this.startString = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            this.startLong = long.Parse(this.startString);
        }

        protected Type GrainType { get; }

        private IDistributedTxStorage transactionStorage;

        protected ILogger Logger { get; private set; }

        /// <summary>
        /// 指标收集器
        /// </summary>
        protected IDTxMetricMonitor MetricMonitor { get; private set; }

        private readonly ConcurrentDictionary<string, Commit<TInput>> inputDict = new ConcurrentDictionary<string, Commit<TInput>>();

        public override async Task OnActivateAsync()
        {
            this.Logger = (ILogger)this.ServiceProvider.GetService(typeof(ILogger<>).MakeGenericType(this.GrainType));
            this.transactionStorage = this.ServiceProvider.GetService<IDistributedTxStorage>();
            this.MetricMonitor = this.ServiceProvider.GetService<IDTxMetricMonitor>();
            var inputList = await this.transactionStorage.GetList<TInput>(this.GrainType.FullName);
            foreach (var input in inputList)
            {
                this.inputDict.TryAdd(input.TransactionId, input);
            }

            this.RegisterTimer(
                async state =>
            {
                foreach (var commit in this.inputDict.Values.ToList())
                {
                    var actors = this.GetTransactionActors(commit.Data);
                    try
                    {
                        await this.AutoCommit(commit, actors);
                    }
                    catch (Exception ex)
                    {
                        await this.Rollback(commit, actors);
                        this.Logger.LogCritical(ex, ex.Message);
                    }
                }
            }, null,
                new TimeSpan(0, 0, 5),
                new TimeSpan(0, 5, 0));
        }

        private string NewID()
        {
            var now_string = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            var now_Long = long.Parse(now_string);
            if (now_Long > this.startLong)
            {
                Interlocked.Exchange(ref this.startString, now_string);
                Interlocked.Exchange(ref this.startLong, now_Long);
                Interlocked.Exchange(ref this.startId, 0);
            }

            var builder = new Span<char>(new char[Length]);
            var newTimes = Interlocked.Increment(ref this.startId);
            if (newTimes <= 99999)
            {
                this.startString.AsSpan().CopyTo(builder);

                var timesString = newTimes.ToString();
                for (int i = this.startString.Length; i < Length - timesString.Length; i++)
                {
                    builder[i] = '0';
                }

                var span = Length - timesString.Length;
                for (int i = span; i < Length; i++)
                {
                    builder[i] = timesString[i - span];
                }

                return builder.ToString();
            }
            else
            {
                return this.NewID();
            }
        }

        protected Task Commit(Commit<TInput> commit)
        {
            return this.Commit(commit, this.GetTransactionActors(commit.Data));
        }

        private async Task Commit(Commit<TInput> commit, IDistributedTx[] actors)
        {
            if (!this.inputDict.ContainsKey(commit.TransactionId))
            {
                commit.Status = TransactionStatus.WaitingCommit;
                await this.transactionStorage.Append(this.GrainType.FullName, commit);
                commit.Status = TransactionStatus.Persistence;
                try
                {
                    await this.AutoCommit(commit, actors);
                }
                catch (Exception ex)
                {
                    if (commit.Status < TransactionStatus.WaitingFinish)
                    {
                        await this.Rollback(commit, actors);
                        throw;
                    }
                    else
                    {
                        this.inputDict.TryAdd(commit.TransactionId, commit);
                    }

                    this.Logger.LogCritical(ex, ex.Message);
                }
            }

            if (this.MetricMonitor != default)
            {
                this.MetricMonitor.Report(new DTxMetricElement
                {
                    Actor = this.GrainType.Name,
                    ElapsedMs = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - commit.Timestamp),
                    IsCommit = true,
                    IsRollback = false
                });
            }
        }

        private async Task Rollback(Commit<TInput> commit, IDistributedTx[] actors)
        {
            if (actors is null || actors.Length == 0)
            {
                throw new NotImplementedException(nameof(this.GetTransactionActors));
            }

            if (commit.Status < TransactionStatus.WaitingFinish)
            {
                await Task.WhenAll(actors.Select(a => a.RollbackTransaction(commit.TransactionId)));
                this.inputDict.TryRemove(commit.TransactionId, out var _);
                if (commit.Status >= TransactionStatus.Persistence)
                {
                    await this.transactionStorage.Delete(this.GrainType.FullName, commit.TransactionId);
                }
            }

            if (this.MetricMonitor != default)
            {
                this.MetricMonitor.Report(new DTxMetricElement
                {
                    Actor = this.GrainType.Name,
                    ElapsedMs = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - commit.Timestamp),
                    IsCommit = false,
                    IsRollback = true
                });
            }
        }

        protected Task Rollback(Commit<TInput> commit)
        {
            return this.Rollback(commit, this.GetTransactionActors(commit.Data));
        }

        private async Task AutoCommit(Commit<TInput> commit, IDistributedTx[] actors)
        {
            if (actors is null || actors.Length == 0)
            {
                throw new NotImplementedException(nameof(this.GetTransactionActors));
            }

            if (commit.Status == TransactionStatus.Persistence ||
                commit.Status == TransactionStatus.WaitingCommit)
            {
                await Task.WhenAll(actors.Select(a => a.CommitTransaction(commit.TransactionId)));
                await this.transactionStorage.Update(this.GrainType.FullName, commit.TransactionId, TransactionStatus.WaitingFinish);
                commit.Status = TransactionStatus.WaitingFinish;
            }

            if (commit.Status == TransactionStatus.WaitingFinish)
            {
                await Task.WhenAll(actors.Select(a => a.FinishTransaction(commit.TransactionId)));
                await this.transactionStorage.Delete(this.GrainType.FullName, commit.TransactionId);
                this.inputDict.TryRemove(commit.TransactionId, out var _);
                commit.Status = TransactionStatus.Finish;
            }
        }

        public async Task<TOutput> Ask(TInput input)
        {
            var commit = new Commit<TInput>
            {
                TransactionId = $"{this.GrainType.Name}{this.NewID()}",
                Status = TransactionStatus.None,
                Data = input,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
            return await this.Work(commit);
        }

        protected abstract IDistributedTx[] GetTransactionActors(TInput input);

        public abstract Task<TOutput> Work(Commit<TInput> commit);
    }
}
