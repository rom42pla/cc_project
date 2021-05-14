# sets the base image
FROM datamechanics/spark:3.1.1-latest
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
# copies the contents
COPY . .
# RUN commands are executed when creating the container
RUN pip install -r requirements.txt
# CMD commands are executed when running the container
CMD ["python", "train.py"]