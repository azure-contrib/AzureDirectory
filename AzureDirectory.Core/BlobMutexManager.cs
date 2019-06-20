using System.Threading;

namespace AzureDirectory.Core
{
    public static class BlobMutexManager
    {
        public static Mutex GrabMutex(string name)
        {
            var mutexName = "luceneSegmentMutex_" + name;

            var notExisting = false;

            if (Mutex.TryOpenExisting(mutexName, out var mutex))
            {
                return mutex;
            }

            // Here we know the mutex either doesn't exist or we don't have the necessary permissions.

            if (!Mutex.TryOpenExisting(mutexName, out mutex))
            {
                notExisting = true;
            }

            return notExisting ? new Mutex(false, mutexName, out _) : Mutex.OpenExisting(mutexName);
        }
    }
}
