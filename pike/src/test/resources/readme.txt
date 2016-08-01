pike -exec -local -ltf tableInfo/tableInfo.xml -ldf tableData/minus_int.txt -f sql/minus.sql -lsec 30
D:\svn\DataDivision\trunk\products\BIPlatform\Java\Pike\pike\src\test\resources
-exec -local -ltf D:\svn\DataDivision\trunk\products\BIPlatform\Java\Pike\pike\src\test\resources\tableInfo.xml -ldf D:\svn\DataDivision\trunk\products\BIPlatform\Java\Pike\pike\src\test\resources\minus_int.txt -f D:\svn\DataDivision\trunk\products\BIPlatform\Java\Pike\pike\src\test\resources\minus.sql -lsec 30 -D pike.output.targets.default=hdfs.minus_int_res1


into hdfs.minus_int_res 