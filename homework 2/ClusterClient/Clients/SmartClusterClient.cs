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
    public class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }
        
        private static readonly ConcurrentDictionary<string, long> ReplicaStats = new();

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var sortedReplicas = ReplicaAddresses
                .OrderBy(uri => ReplicaStats.GetOrAdd(uri, 0))
                .ToList();
            
            var tasks = new List<Task<ResponseData>>();
            var perReplicaTimeout = TimeSpan.FromMilliseconds(timeout.TotalMilliseconds / ReplicaAddresses.Length);
            var sw = Stopwatch.StartNew();

            for (int i = 0; i < sortedReplicas.Count; i++)
            {
                var request = CreateRequest(sortedReplicas[i] + "?query=" + query);
                tasks.Add(ProcessAndMeasureAsync(sortedReplicas[i], request));
                
                while (true)
                {
                    var timePassed = sw.Elapsed;
                    var timeToNextLaunch = perReplicaTimeout * (i + 1) - timePassed;
                    
                    if (i == ReplicaAddresses.Length - 1)
                        timeToNextLaunch = timeout - timePassed;

                    var delayTask = Task.Delay(timeToNextLaunch);
                    var completedTask = await Task.WhenAny(Task.WhenAny(tasks), delayTask);

                    if (completedTask != delayTask)
                    {
                        var finishedTask = await (Task<Task<ResponseData>>)completedTask;
                        try
                        {
                            var response = await finishedTask;
                            UpdateStats(response.Uri, response.ElapsedMs);
                            return response.Content;
                        }
                        catch (Exception)
                        {
                            UpdateStats(sortedReplicas[i], (long)timeout.TotalMilliseconds);
                            tasks.Remove(finishedTask);
                            if (tasks.Count == 0 || i < ReplicaAddresses.Length - 1)
                            {
                                goto next;
                            }
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                next: ;
            }

            throw new TimeoutException();
        }
        
        private async Task<ResponseData> ProcessAndMeasureAsync(string uri, System.Net.WebRequest request)
        {
            var timer = Stopwatch.StartNew();
            var content = await ProcessRequestAsync(request);
            return new ResponseData { Uri = uri, Content = content, ElapsedMs = timer.ElapsedMilliseconds };
        }
        private void UpdateStats(string uri, long elapsedMs)
        {
            ReplicaStats.AddOrUpdate(uri, elapsedMs, (key, oldVal) => (oldVal + elapsedMs) / 2);
        }
        
        private class ResponseData
        {
            public string Uri { get; set; }
            public string Content { get; set; }
            public long ElapsedMs { get; set; }
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}