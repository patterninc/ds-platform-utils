# metrics-server — the resource metrics API behind `kubectl top`. EKS does
# not install it. Steps declare @resources and Kueue charges quota against
# the request, not the usage, so without this there is no way to see whether
# a step is over-requesting.
#
# Short-window aggregator (~2-5 min), not a store.
#
# NO METRICS SCRAPER YET. Karpenter (:8080/metrics) and Kueue (:8443/metrics,
# via the chart's metricsService) both serve Prometheus endpoints, but
# nothing polls them, so values are live-only and reset on pod restart.
# Reachable by hand:
#
#   kubectl -n kube-system port-forward deploy/karpenter 8080:8080
#   curl -s localhost:8080/metrics | grep karpenter_nodes
#
# kube-prometheus-stack is the intended route — it brings the Operator CRDs,
# after which Karpenter's `serviceMonitor.enabled` and Kueue's
# `enablePrometheus` wire themselves up. Budget ~850m CPU and ~3Gi on the
# system nodes plus a PVC for the TSDB.

resource "helm_release" "metrics_server" {
  name       = "metrics-server"
  namespace  = "kube-system"
  repository = "https://kubernetes-sigs.github.io/metrics-server/"
  chart      = "metrics-server"
  version    = var.metrics_server_version

  values = [yamlencode({
    # One replica. A metrics-server outage costs `kubectl top` and nothing
    # else — no workload depends on it, since we run no HPAs — so paying for
    # HA here would be buying availability nothing consumes.
    replicas = 1

    resources = {
      requests = { cpu = "50m", memory = "128Mi" }
      limits   = { memory = "256Mi" }
    }

    # Same reasoning as Karpenter and Kueue: keep cluster-level services on
    # the always-on node group rather than on a node Karpenter may reclaim.
    nodeSelector = {
      "pattern-ml-platform.pattern.com/role" = "system"
    }

    # InternalIP first. Our nodes have no public addresses and Bottlerocket
    # does not register a resolvable hostname, so the other address types in
    # the chart default would each be tried and time out first.
    args = [
      "--kubelet-preferred-address-types=InternalIP",
    ]
  })]

  depends_on = [module.eks]
}
