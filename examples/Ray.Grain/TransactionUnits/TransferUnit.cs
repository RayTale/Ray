﻿using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Ray.DistributedTransaction;
using Ray.IGrains.Actors;
using Ray.IGrains.TransactionUnits;
using Ray.IGrains.TransactionUnits.Inputs;

namespace Ray.Grain.TransactionUnits
{
    public class TransferUnit : DistributedTxUnit<TransferInput, bool>, ITransferUnit
    {
        public TransferUnit(ILogger<TransferUnit> logger) : base(logger)
        {
        }
        public override IDistributedTx[] GetTransactionActors(TransferInput input)
        {
            return new IDistributedTx[]
            {
                GrainFactory.GetGrain<IAccount>(input.FromId),
                GrainFactory.GetGrain<IAccount>(input.ToId),
            };
        }

        public override async Task<(bool needCommit, bool needRollback, bool output)> Work(Commit<TransferInput> commit)
        {
            var result = await GrainFactory.GetGrain<IAccount>(commit.Data.FromId).TransferDeduct(commit.Data.Amount, commit.TransactionId);
            if (result)
            {
                await GrainFactory.GetGrain<IAccount>(commit.Data.ToId).TransferAddAmount(commit.Data.Amount, commit.TransactionId);
                return (true, false, true);
            }
            return (false, false, false);
        }
    }
}
