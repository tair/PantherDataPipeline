git fetch -a
git pull origin develop
mvn clean package -DskipTests
mv target/ROOT.war ec2-user@54.68.67.235:/home/ec2-user/softwares/apache-tomcat-9.0.22/webapps/.