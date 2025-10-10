FROM flink:1.20.0-scala_2.12

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install apache-flink==1.20.0 && \
    ln -s /usr/bin/python3 /usr/bin/python

# Optionally copy jobs/jars in during build (if desired)
COPY flink_jobs /opt/flink/user_jobs/
COPY flink_jars/* /opt/flink/lib/
