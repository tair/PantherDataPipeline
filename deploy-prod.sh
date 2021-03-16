mvn clean package -DskipTests
scp -i ~/.ssh/phylogenes-dev.pem -r target/ROOT.war ec2-user@52.37.99.223:/home/ec2-user/softwares/apache-tomcat-9.0.22/webapps/.