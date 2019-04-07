using System;

namespace Ray.Core.Exceptions
{
    public class TxSnapshotException : Exception
    {
        public TxSnapshotException(string stateId, long snapshotVersion, long backupSnapshotVersion) :
            base($"StateId {stateId} and snapshot version {snapshotVersion} and backup snapshot version {backupSnapshotVersion}")
        {
        }
    }
}
