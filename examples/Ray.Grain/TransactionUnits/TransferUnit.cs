using Ray.DistributedTx;
using Ray.IGrains.Actors;
using Ray.IGrains.TransactionUnits;
using Ray.IGrains.TransactionUnits.Inputs;
using System.Threading.Tasks;

namespace Ray.Grain.TransactionUnits
{
    public class TransferUnit : DTxUnitGrain<TransferInput, bool>, ITransferUnit
    {
        public override IDistributedTx[] GetTransactionActors(TransferInput input)
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
                    await GrainFactory.GetGrain<IAccount>(commit.Data.ToId).TransferAddAmount(commit.Data.Amount, commit.TransactionId);
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
