# Copy From A CSV File To A Database

It was made using Java 17 and MS SQL (it shouldn't be a problem to change it to some other SQL)

Kept getting an "Out of memory exception" in SQL Server Management Studio for a particularly large CSV file, so made this to do the same thing with Java.

It creates a table by taking the first row data from the file and assigning it as table column names and then it looks at the second row data and tries to parse it to basic data types (DATE, INT, DECIMAL), otherwise it makes it into a VARCHAR. It can be easily expanded by using some regex (or changed to other locales for dates).

First you will need to download a database driver, for example: mssql-jdbc-10.2.1.jre17.jar It needs to be compiled with the -cp "Path to your JDBC jar" command to work. You could also go to the Environment Variables and add a Variable name of "CLASSPATH" and Variable value: "Path to your JDBC jar, for example: C:\Program Files\Microsoft JDBC DRIVER 10.2 for SQL Server\sqljdbc_10.2\enu\mssql-jdbc-10.2.1.jre17.jar"