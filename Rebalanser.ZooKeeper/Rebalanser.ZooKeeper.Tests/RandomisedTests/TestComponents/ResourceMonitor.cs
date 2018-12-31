using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Rebalanser.ZooKeeper.Tests.RandomisedTests.TestComponents
{
    public class ResourceMonitor
    {
        private Dictionary<string, string> resources;
        private HashSet<string> removedResources;
        private List<object> violations;
        private ConcurrentQueue<AssignmentEvent> assignmentEvents;

        public ResourceMonitor()
        {
            this.resources = new Dictionary<string, string>();
            this.violations = new List<object>();
            this.assignmentEvents = new ConcurrentQueue<AssignmentEvent>();
            this.removedResources = new HashSet<string>();
        }

        public void CreateResource(string resourceName)
        {
            this.resources.Add(resourceName, "");
        }

        public List<object> GetDoubleAssignments()
        {
            return this.violations;
        }

        public bool DoubleAssignmentsExist()
        {
            return this.violations.Any();
        }
        
        public bool AllResourcesAssigned()
        {
            var unassigned = this.resources.Where(x => x.Value == string.Empty).ToList();
            if (unassigned.Any())
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}Unassigned resources: {string.Join(",", unassigned)}");
                return false;
            }

            return true;
        }
 
        public void Clear()
        {
            this.resources.Clear();
        }

        public void ClaimResource(string resourceName, string clientId)
        {
            this.assignmentEvents.Enqueue(new AssignmentEvent()
            {
                EventTime = DateTime.Now,
                ClientId = clientId,
                Action = $"Assign {resourceName}"
            });
            if (this.resources.ContainsKey(resourceName))
            {
                string currValue = this.resources[resourceName];
                if (currValue.Equals(string.Empty))
                {
                    this.resources[resourceName] = clientId;
                }
                else
                {
                    var violation = new ClaimViolation(resourceName, currValue, clientId);
                    this.assignmentEvents.Enqueue(new AssignmentEvent()
                    {
                        ClientId = clientId,
                        Action = violation.ToString(),
                        EventTime = DateTime.Now
                    });
                    this.violations.Add(violation);
                }
            }
        }
        
        public void ReleaseResource(string resourceName, string clientId)
        {
            this.assignmentEvents.Enqueue(new AssignmentEvent()
            {
                EventTime = DateTime.Now,
                ClientId = clientId,
                Action = $"Release {resourceName}"
            });

            if (this.resources.ContainsKey(resourceName))
            {
                string currValue = this.resources[resourceName];
                if (currValue.Equals(clientId))
                {
                    this.resources[resourceName] = string.Empty;
                }
                else if (currValue.Equals(string.Empty))
                {
                    // fine
                }
                else
                {
                    var violation = new ReleaseViolation(resourceName, currValue, clientId);
                    this.assignmentEvents.Enqueue(new AssignmentEvent()
                    {
                        ClientId = clientId,
                        Action = violation.ToString(),
                        EventTime = DateTime.Now
                    });
                    this.violations.Add(violation);
                }
            }
        }

        public void AddResource(string resourceName)
        {
            this.resources.Add(resourceName, string.Empty);
            this.assignmentEvents.Enqueue(new AssignmentEvent()
            {
                EventTime = DateTime.Now,
                ClientId = "-",
                Action = $"Add Resource - {resourceName}"
            });
        }
        
        public void RemoveResource(string resourceName)
        {
            this.resources.Remove(resourceName);
            this.removedResources.Add(resourceName);
            this.assignmentEvents.Enqueue(new AssignmentEvent()
            {
                EventTime = DateTime.Now,
                ClientId = "-",
                Action = $"Remove Resource - {resourceName}"
            });
        }
        
        public void RegisterAddClient(string clientId)
        {
            this.assignmentEvents.Enqueue(new AssignmentEvent()
            {
                EventTime = DateTime.Now,
                ClientId = "-",
                Action = $"Add Client - {clientId}"
            });
        }
        
        public void RegisterRemoveClient(string clientId)
        {
            this.assignmentEvents.Enqueue(new AssignmentEvent()
            {
                EventTime = DateTime.Now,
                ClientId = "-",
                Action = $"Remove Client - {clientId}"
            });
        }

        public void PrintEvents(string path)
        {
            var lines = new List<string>();

            while (true)
            {
                AssignmentEvent evnt = null;
                if (this.assignmentEvents.TryDequeue(out evnt))
                    lines.Add($"{evnt.EventTime.ToString("hh:mm:ss,fff")}|{evnt.ClientId}|{evnt.Action}");
                else
                    break;
            }

            var resList = new List<KeyValuePair<string, string>>(this.resources.ToList());
            lines.Add($"||---- Resource Assignment State -----");
            foreach(var kv in resList)
                lines.Add($"||{kv.Key}->{kv.Value}");
            lines.Add("||------------------------------------");
            
            if(!File.Exists(path))
                File.WriteAllText(path, "Time|Client|Action"+Environment.NewLine);
            
            File.AppendAllLines(path, lines);
        }
    }
}