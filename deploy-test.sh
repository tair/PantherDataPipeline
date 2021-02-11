mvn clean package -DskipTests
scp -i ~/.ssh/phylogenes-dev.pem -r target/ROOT.war ec2-user@54.68.67.235:/home/ec2-user/softwares/apache-tomcat-9.0.22/webapps/.