rm *.log
cd ../;
clean-eclipse; 
mvn clean compile install -DskipTests eclipse:eclipse
cd -
