FROM java:8-jdk
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java","-jar", "final-project-runable.jar"]

