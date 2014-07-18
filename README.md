#Presto OData Service

Work in progress for an OData service implementation on top of Presto using Presto's JDBC driver. The ultimate goal is to have a proper OData service that can be integrated with other tools such as Tableau.

#Testing

- Create a table in Hive:  
drop table addresses;  
CREATE TABLE addresses (street STRING, city STRING, apt INT)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;    
LOAD DATA LOCAL INPATH '/tmp/address.txt' into table addresses;

- cat /tmp/address.txt   
s1,c1,1   
s2,c2,2  
s3,c3,3  
s4,c4,4   
s5,c5,5  

Start the service:  
mvn exec:java

Try the following URLs:  
[http://localhost:8888/PrestoODataService.svc/$metadata?$format=json](http://localhost:8888/PrestoODataService.svc/$metadata?$format=json)  
[http://localhost:8888/PrestoODataService.svc/addresses?$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$format=json)    
[http://localhost:8888/PrestoODataService.svc/addresses?$filter=city eq 'c4' and street eq 's1'&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$filter=city eq 'c4' and street eq 's1'&$format=json)  
[http://localhost:8888/PrestoODataService.svc/addresses?$filter=city eq 'c4'&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$filter=city eq 'c4'&$format=json)    
[http://localhost:8888/PrestoODataService.svc/addresses?$filter=apt eq 5&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$filter=apt eq 5&$format=json)
[http://localhost:8888/PrestoODataService.svc/addresses?$orderby=apt desc&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$orderby=apt desc&$format=json)
[http://localhost:8888/PrestoODataService.svc/addresses?$orderby=apt asc&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$orderby=apt asc&$format=json)
[http://localhost:8888/PrestoODataService.svc/addresses/$count](http://localhost:8888/PrestoODataService.svc/addresses/$count)
[http://localhost:8888/PrestoODataService.svc/addresses?$select=city&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$select=city&$format=json)
[http://localhost:8888/PrestoODataService.svc/addresses?$select=city,apt&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$select=city,apt&$format=json)
[http://localhost:8888/PrestoODataService.svc/addresses?$select=city,apt,street&$format=json](http://localhost:8888/PrestoODataService.svc/addresses?$select=city,apt,street&$format=json)

#Dependencies
odata4j 0.70 (http://odata4j.org/)   
presto-jdbc 0.70

#References
[OData v4.0 Spec](http://www.odata.org/documentation/odata-version-4-0/)
