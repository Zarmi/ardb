killall ardb-server
rm -rf ../data
../../src/ardb-server ../ardb-test.conf
sleep 1
mvn test
killall ardb-server
