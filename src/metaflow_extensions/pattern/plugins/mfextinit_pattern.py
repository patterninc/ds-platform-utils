###
# Plugins contributed by ds-platform-utils.
#
# Entries are (name, class_path) tuples; a leading "." is relative to this
# package. Metaflow discovers this file through the `metaflow_extensions`
# namespace package, which is why no directory above `plugins` has an
# __init__.py -- the namespace is shared with every other installed extension
# (Outerbounds ships several), and an __init__.py would hide theirs or ours.
###

CLIS_DESC = []

FLOW_DECORATORS_DESC = [
    ("uv_base", ".uv_decorators.UVFlowDecorator"),
]

###
# @uv gives a *local* step its own uv environment, scoped to the dependency
# group it declares. Remote steps are handled elsewhere: ds_platform_utils'
# @uv_base mutator bakes them an image. This has to be a real StepDecorator
# rather than a mutator because retargeting a local interpreter happens in
# runtime_step_cli, which the mutator API does not expose.
###
STEP_DECORATORS_DESC = [
    ("uv", ".uv_decorators.UVStepDecorator"),
]

ENVIRONMENTS_DESC = []

DATASTORES_DESC = []

METADATA_PROVIDERS_DESC = []

SIDECARS_DESC = []

LOGGING_SIDECARS_DESC = []

MONITOR_SIDECARS_DESC = []

AWS_CLIENT_PROVIDERS_DESC = []

__mf_promote_submodules__ = []
