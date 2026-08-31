# Ship this package inside Metaflow's code package. Without it a flow that imports
# ds_platform_utils from a local checkout fails in an isolated @pypi environment: Metaflow
# auto-includes only the modules that define decorators, so the package arrives as a namespace
# package with no __init__ and the re-exports are missing. Flows that install ds-platform-utils
# as a real dependency do not need this, but developing against a checkout does.
METAFLOW_PACKAGE_POLICY = "include"
