# PySparkOnK8s

An Airflow provider that turns `@task.pyspark` tasks into client-mode Spark drivers on Kubernetes. The Airflow worker pod becomes the Spark driver, and the executors are Kubernetes pods that Spark creates and deletes itself.

Documentation is coming at https://sdaberdaku.github.io/pysparkonk8s/.
