using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }
        
        private static readonly ConcurrentDictionary<string, long> ReplicaStats = new();

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var sortedReplicas = ReplicaAddresses
                .OrderBy(uri => ReplicaStats.GetOrAdd(uri, 0))
                .ToList();
            var sw = Stopwatch.StartNew();
            
            for (int i = 0; i < sortedReplicas.Count; i++)
            {
                var remainingReplicas = sortedReplicas.Count - i;
                var timeLeft = timeout - sw.Elapsed;
                var currentReplicaTimeout = TimeSpan.FromMilliseconds(timeLeft.TotalMilliseconds / remainingReplicas);
                var uri = sortedReplicas[i];
                var request = CreateRequest(uri + "?query=" + query);
                var requestTimer = Stopwatch.StartNew();
                var task = ProcessRequestAsync(request);
                var delayTask = Task.Delay(currentReplicaTimeout);
                var completedTask = await Task.WhenAny(task, delayTask);
                
                if (completedTask == task)
                {
                    try
                    {
                        var result = await task;
                        UpdateStats(uri, requestTimer.ElapsedMilliseconds);
                        return result;
                    }
                    catch (Exception)
                    {
                        UpdateStats(uri, (long)timeout.TotalMilliseconds);
                    }
                }
            }
            throw new TimeoutException($"Request {query} timed out");
        }
        
        private void UpdateStats(string uri, long time)
        {
            ReplicaStats.AddOrUpdate(uri, time, (key, oldVal) => (oldVal + time) / 2);
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
