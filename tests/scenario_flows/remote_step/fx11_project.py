"""@project context inside the pod.

`current.is_production` decides which schema user code writes to, and an
unset one reads as False -- so without forwarding, a production run wrote to
staging silently, with no error anywhere.
"""

from metaflow import FlowSpec, current, project, remote_step, resources, step

from _check import check


@project(name="remote_step_probe")
class Fx11Project(FlowSpec):
    @step
    def start(self):
        # What the driver sees, to compare against the pod.
        self.driver_view = {
            "project_name": current.project_name,
            "branch_name": current.branch_name,
            "is_production": current.is_production,
            "project_flow_name": current.project_flow_name,
        }
        print(f"[fx11] driver: {self.driver_view}", flush=True)
        self.next(self.work)

    @remote_step
    @resources(cpu=1, memory=4000)
    @step
    def work(self):
        pod_view = {
            "project_name": current.project_name,
            "branch_name": current.branch_name,
            "is_production": current.is_production,
            "project_flow_name": current.project_flow_name,
        }
        print(f"[fx11] pod:    {pod_view}", flush=True)
        check("project_name matches the driver", pod_view["project_name"], self.driver_view["project_name"])
        check("branch_name matches the driver", pod_view["branch_name"], self.driver_view["branch_name"])
        check("is_production matches the driver", pod_view["is_production"], self.driver_view["is_production"])
        check("project_flow_name matches", pod_view["project_flow_name"], self.driver_view["project_flow_name"])
        check("project_name is not empty", pod_view["project_name"], "remote_step_probe")
        self.pod_view = pod_view
        self.next(self.end)

    @step
    def end(self):
        check("pod context came back", self.pod_view["project_name"], "remote_step_probe")
        print("[fx11] OK")


if __name__ == "__main__":
    Fx11Project()
