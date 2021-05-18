# sets the base image
FROM amazoncorretto:8

# installs dependencies for the base image
RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv

# RUN commands are executed when creating the container
RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

# copies the contents
COPY . .

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# CMD commands are executed when running the container
CMD ["python3", "train.py"]