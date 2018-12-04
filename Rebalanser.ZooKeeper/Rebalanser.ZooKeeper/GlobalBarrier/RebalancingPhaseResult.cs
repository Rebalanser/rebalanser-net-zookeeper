using Rebalanser.ZooKeeper.Zk;

namespace Rebalanser.ZooKeeper.GlobalBarrier
{
    public class RebalancingPhaseResult
    {
        public RebalancingPhaseResult(RebalancingResult phaseResult)
        {
            PhaseResult = phaseResult;
        }
        
        public ClientsZnode ClientsZnode { get; set; }
        public ResourcesZnode ResourcesZnode { get; set; }
        public RebalancingResult PhaseResult { get; set; }
    }
}