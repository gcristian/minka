rm *.log
cd ../;
#clean-eclipse; 
mvn compile install -DskipTests 
#eclipse:eclipse
cd -
