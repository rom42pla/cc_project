# Cloud Computing project

## Who are we?

| Name | Matricola |
| --- | --- |
| Martina Betti | 1799160 |
| Federico Fontana | 1744946 |
| Romeo Lanzino | 1753403 |
| Stefania Sferragatta | 1948081 |

## How-tos

### How to setup the environment

The project is built upon Python 3.8 using the PySpark package.

We recommend installing [Anaconda](https://www.anaconda.com/products/individual), which comes bundled with many useful
modules and tools such as the virtual environments.

After Anaconda is installed, you can install Python's dependencies with:

```bash
pip install -r requirements.txt
```

At this point you should have the correct environment to interact with the scripts in this project.

### How to train the model

Be sure to have the dependencies installed and just type:

```bash
python train.py
```

### How to build and run the Docker container

To **build** the container, from the root folder (the one with `Dockerfile`, `requirements.txt` etc) type:

```bash
bash scripts/docker_build.sh
```

To **run** the container, from the root folder type:

```bash
bash scripts/docker_run.sh
```
To run the preprocessing and create the scheduler:

```bash 
bash s3_bucket/preprocessing.py 
```

notice: config file, test files and output examples are provided

To train:

```bash 
bash s3_bucket/train_batch.py -d $day -i $iterations
```

notice: not working, must adapt training script (to be discussed)

