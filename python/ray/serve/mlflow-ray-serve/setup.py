import setuptools

setuptools.setup(
    name="mlflow-ray-serve",
    version="0.0.1",
    description="Ray Serve MLflow deployment plugin",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ray-project/mlflow-ray-serve",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    install_requires=["ray[serve]", "mlflow>=1.12.0"],
    entry_points={"mlflow.deployments": "ray-serve=mlflow_ray_serve"})
