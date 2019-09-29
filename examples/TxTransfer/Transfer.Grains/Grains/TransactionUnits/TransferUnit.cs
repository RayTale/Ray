using Ray.DistributedTx;
using Ray.DistributedTx.Abstractions;
using System.Threading.Tasks;
using Transfer.IGrains;
using Transfer.IGrains.TransactionUnits;

namespace Transfer.Grains.Grains.TransactionUnits
{
    public class TransferUnit : DTxUnitGrain<TransferInput, bool>, ITransferUnit
    {
        protected override IDistributedTx[] GetTransactionActors(TransferInput input)
        {
            return new IDistributedTx[]
            {
                GrainFactory.GetGrain<IAccount>(input.FromId),
                GrainFactory.GetGrain<IAccount>(input.ToId),
            };
        }

        public override async Task<bool> Work(Commit<TransferInput> commit)
        {
            try
            {
                var result = await GrainFactory.GetGrain<IAccount>(commit.Data.FromId).TransferDeduct(commit.Data.Amount, commit.TransactionId);
                if (result)
                {
                    await GrainFactory.GetGrain<IAccount>(commit.Data.ToId).TransferArrived(commit.Data.Amount, commit.TransactionId);
                    await Commit(commit);
                    return true;
                }
                else
                {
                    await Rollback(commit);
                    return false;
                }
            }
            catch
            {
                await Rollback(commit);
                throw;
            }
        }
    }
}
