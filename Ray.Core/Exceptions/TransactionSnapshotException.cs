using System;

namespace Ray.Core.Exceptions
{
    public class TransactionSnapshotException : Exception
    {
        public TransactionSnapshotException(string stateId, long snapshotVersion, long backupSnapshotVersion) :
            base($"StateId {stateId} and snapshot version {snapshotVersion} and backup snapshot version {backupSnapshotVersion}")
        {
        }
    }
}
