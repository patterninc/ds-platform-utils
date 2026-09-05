# Kueue — job queueing, quotas and fair sharing across teams.
#
# Kueue sits in front of the Job controller: a Job labelled with a LocalQueue
# is held as a Workload until its ClusterQueue has quota, then admitted. That
# is what stops one team's backfill from consuming the whole fleet, and what
# lets an idle team's capacity be borrowed rather than sitting cold.

resource "helm_release" "kueue" {
  name             = "kueue"
  namespace        = "kueue-system"
  create_namespace = true

  repository = "oci://registry.k8s.io/kueue/charts"
  chart      = "kueue"
  version    = var.kueue_version

  values = [yamlencode({
    controllerManager = {
      manager = {
        resources = {
          requests = { cpu = "500m", memory = "512Mi" }
          limits   = { memory = "512Mi" }
        }
      }
      # Pin to the always-on system nodes for the same reason as Karpenter:
      # the thing that admits workloads should not depend on a node that a
      # workload-driven autoscaler might reclaim.
      nodeSelector = {
        "pattern-ml-platform.pattern.com/role" = "system"
      }
    }

    # controllerManagerConfigYaml REPLACES the chart's default wholesale — it
    # is not merged. So everything the default sets has to be restated here,
    # not just our changes. Dropping `health` alone breaks the chart's
    # liveness and readiness probes (they target :8081) and crashloops the
    # controller; dropping `metrics` silently removes the metrics endpoint.
    #
    # Keep this in sync when bumping var.kueue_version:
    #   helm show values oci://registry.k8s.io/kueue/charts/kueue \
    #     --version <v> | sed -n '/controllerManagerConfigYaml/,/^[a-z]/p'
    managerConfig = {
      controllerManagerConfigYaml = yamlencode({
        # v1beta2 as of Kueue 0.19 — v1beta1 is the older config API and is
        # not what this chart's controller expects.
        apiVersion = "config.kueue.x-k8s.io/v1beta2"
        kind       = "Configuration"

        health = {
          healthProbeBindAddress = ":8081"
        }
        metrics = {
          # Serves Prometheus metrics — ClusterQueue quota usage, admission
          # latency, pending Workload counts. metricsService (a ClusterIP on
          # 8443) is created by the chart and fronts this.
          bindAddress = ":8443"
          # Per-ClusterQueue resource usage series. Off by default because it
          # is per-queue-per-flavor-per-resource cardinality; with six teams
          # that is small, and it is the number you actually want when asking
          # "is forecasting starving nlp?".
          enableClusterQueueResources = true
        }
        webhook = {
          port = 9443
        }
        leaderElection = {
          leaderElect  = true
          resourceName = "c1f6bfd2.kueue.x-k8s.io"
        }
        controller = {
          groupKindConcurrency = {
            "Job.batch"                     = 5
            "Pod"                           = 5
            "Workload.kueue.x-k8s.io"       = 10
            "LocalQueue.kueue.x-k8s.io"     = 5
            "ClusterQueue.kueue.x-k8s.io"   = 5
            "ResourceFlavor.kueue.x-k8s.io" = 1
          }
        }
        clientConnection = {
          # Chart defaults. Well above client-go's 5/10, which would throttle
          # the controller during a burst of admissions.
          qps   = 300
          burst = 500
        }

        integrations = {
          # batch/v1 Job is the only shape we submit. The chart default also
          # enables MPIJob, RayJob/RayService/RayCluster and JobSet; leaving
          # those off keeps the controller's watch set small and avoids it
          # erroring on CRDs that are not installed.
          frameworks = ["batch/job"]
        }
        # A Workload that cannot get all its pods running within the timeout
        # is evicted and requeued rather than holding quota indefinitely. Our
        # steps are single-pod, so this mostly guards against a node that
        # never arrives (capacity exhaustion in every AZ).
        #
        # No `enable` key: v1beta1 had one, v1beta2 dropped it — the feature
        # is on by virtue of this block being present. Kueue decodes its
        # config strictly, so an `enable: true` here is not ignored, it kills
        # the controller at startup:
        #   strict decoding error: unknown field "waitForPodsReady.enable"
        waitForPodsReady = {
          timeout        = "15m"
          blockAdmission = false
        }
      })
    }
  })]

  depends_on = [module.eks]
}

# ---------------------------------------------------------------------------
# ResourceFlavors + priority classes — cluster-wide, team-independent
# ---------------------------------------------------------------------------

data "kubectl_file_documents" "kueue_flavors" {
  content = file("${path.module}/kueue-flavors.yaml")
}

resource "kubectl_manifest" "kueue_flavors" {
  for_each  = data.kubectl_file_documents.kueue_flavors.manifests
  yaml_body = each.value

  depends_on = [helm_release.kueue]
}

# ---------------------------------------------------------------------------
# Per-team namespace, ClusterQueue, LocalQueue, runner ServiceAccount
#
# One rendered document set per entry in var.teams. Adding a team is a
# variable change, not new Terraform.
# ---------------------------------------------------------------------------

data "kubectl_file_documents" "kueue_teams" {
  for_each = var.teams

  content = templatefile("${path.module}/kueue-team.yaml.tpl", {
    team          = each.key
    cpu_quota     = each.value.cpu_quota
    cpu_borrow    = each.value.cpu_borrow
    memory_quota  = each.value.memory_quota
    memory_borrow = each.value.memory_borrow
    gpu_quota     = each.value.gpu_quota
    gpu_borrow    = each.value.gpu_borrow
  })
}

locals {
  # Flatten {team => {doc_id => yaml}} into a single keyed map so each
  # document is its own resource instance.
  kueue_team_docs = merge([
    for team, docs in data.kubectl_file_documents.kueue_teams : {
      for doc_id, body in docs.manifests : "${team}/${doc_id}" => body
    }
  ]...)

  # Namespaces are split out from everything else because terraform creates
  # for_each instances in PARALLEL with no ordering between them. Applied as
  # one set, a namespaced object races its own Namespace and the apply fails
  # intermittently:
  #
  #   Error: content/remote-step-runner failed to run apply:
  #   namespaces "content" not found
  #
  # Chaining the two with depends_on makes the ordering explicit.
  #
  # Matched on the rendered body rather than the document id so it keeps
  # working if the template gains another cluster-scoped object.
  kueue_team_namespaces = {
    for k, v in local.kueue_team_docs : k => v
    if can(regex("(?m)^kind:[[:space:]]+Namespace[[:space:]]*$", v))
  }

  kueue_team_members = {
    for k, v in local.kueue_team_docs : k => v
    if !can(regex("(?m)^kind:[[:space:]]+Namespace[[:space:]]*$", v))
  }
}

resource "kubectl_manifest" "kueue_team_namespaces" {
  for_each = local.kueue_team_namespaces

  yaml_body = each.value

  depends_on = [kubectl_manifest.kueue_flavors]
}

# ClusterQueues, LocalQueues and runner ServiceAccounts. The LocalQueue and
# ServiceAccount are namespace-scoped, so these must land after the
# namespaces above exist.
resource "kubectl_manifest" "kueue_teams" {
  for_each = local.kueue_team_members

  yaml_body = each.value

  depends_on = [kubectl_manifest.kueue_team_namespaces]
}
