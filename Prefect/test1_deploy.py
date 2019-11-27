from dags.test1_dag import flow

# all other init kwargs to `Docker` are accepted here
flow.deploy(
    "Bmez Trial",
    registry_url="216367352155.dkr.ecr.us-east-1.amazonaws.com/analytics-docker-repo",
    base_image="python:3.7",
)
