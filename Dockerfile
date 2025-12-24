FROM flink:1.18.1-java11

# 1. Install System Dependencies
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python

# 2. Install Python Libraries (Pandas, Scikit-Learn, PinotDB, Kafka)
RUN pip3 install apache-flink==1.18.1 kafka-python pandas scikit-learn pinotdb

# 3. Download Kafka Connector JAR (to /opt/flink/lib so it's always available)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# 4. Fix: Copy Flink Python JAR from opt/ to lib/ (Solves the "PythonScalarFunctionOperator" error)
RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/

# 5. Set working directory
WORKDIR /opt/flink
