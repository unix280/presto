FROM amazonlinux:2022
RUN yum groupinstall -y "Development Tools" && yum install -y tree

RUN mkdir -p /app
COPY /cpp /app/
RUN tree /app && /app/scripts/setup-amazon2022.sh
