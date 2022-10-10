import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadFromCSV {
    private static final String FILE_LOCATION = "D://Test.csv";
    private static final String JDBC_DRIVER = "jdbc:sqlserver:";
    private static final String SERVER_NAME = "localhost";
    private static final String DATABASE_NAME = "TestDB";
    private static final String USER_NAME = "test";
    private static final String PASSWORD = "test";
    private static final String TABLE_NAME = "dbo.TestCreate";
    private static final Path path = Paths.get(FILE_LOCATION);
    private static final String url = JDBC_DRIVER + "//" + SERVER_NAME + ";databaseName=" + DATABASE_NAME + ";trustServerCertificate=true;user=" + USER_NAME + ";password=" + PASSWORD + ";";
    private static final int batchSize = 1000;

    public static void main(String[] args) throws Exception {

        List<String> tableColumnNames;
        List<String> dataTypes = new ArrayList<>();
        Queue<List<String>> data = new ConcurrentLinkedDeque<>();
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE " + TABLE_NAME + " ( Id INT IDENTITY(1,1) PRIMARY KEY, ");
        StringBuilder insertQuery = new StringBuilder("INSERT INTO " + TABLE_NAME + " (");

        data = getData(data);

        tableColumnNames = data.poll();

        assert data.peek() != null;
        determineDataTypes(data.peek(), dataTypes);

        assert tableColumnNames != null;
        createTableQuery = getTableQuery(tableColumnNames, dataTypes, createTableQuery);

        insertQuery = getInsertQuery(tableColumnNames, insertQuery);

        executeQueries(createTableQuery, insertQuery, data, dataTypes);
    }

    /*
     * Get the data from the CSV file
     */
    private static Queue<List<String>> getData(Queue<List<String>> data) {
        try (Stream<String> file = Files.lines(path)) {
            data =
                file
                    .parallel()
                    .map(x -> Arrays.asList(x.split(",")))
                    .collect(Collectors.toCollection(LinkedList::new));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    /*
     * Determine the data types of the
     * columns in the CSV file
     */
    private static void determineDataTypes(List<String> tableDataSample, List<String> dataTypes) {
        for (String s : tableDataSample) {
            if (s.matches("\\d+")) {
                dataTypes.add("INTEGER");
            } else if (s.matches("\\d+\\.\\d+")) {
                dataTypes.add("DECIMAL(36,18)");
            } else if (s.matches("\\d{1,2}-\\d{1,2}-\\d{4}")) {
                dataTypes.add("DATE");
            } else if (s.matches("\\d{4}-\\d{1,2}-\\d{1,2}")) {
                dataTypes.add("DATE");
            } else if (s.matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
                dataTypes.add("DATE");
            } else if (s.matches("\\d{4}/\\d{1,2}/\\d{1,2}")) {
                dataTypes.add("DATE");
            } else {
                dataTypes.add("VARCHAR(255)");
            }
        }
    }

    /*
     * Create the string for the create table query
     */
    private static StringBuilder getTableQuery(List<String> tableColumnNames, List<String> dataTypes, StringBuilder createTableQuery) {
        for (int i = 0; i < tableColumnNames.size(); i++) {
            createTableQuery.append("[").append(tableColumnNames.get(i)).append("]").append(" ").append(dataTypes.get(i)).append(", ");
        }
        createTableQuery = new StringBuilder(createTableQuery.substring(0, createTableQuery.length() - 2));
        createTableQuery.append(")");
        return createTableQuery;
    }

    /*
     * Create the string for the insert table query
     */
    private static StringBuilder getInsertQuery(List<String> tableColumnNames, StringBuilder insertQuery) {
        for (String tableColumnName : tableColumnNames) {
            insertQuery.append("[").append(tableColumnName).append("]").append(", ");
        }
        insertQuery = new StringBuilder(insertQuery.substring(0, insertQuery.length() - 2));
        insertQuery.append(") VALUES (");
        insertQuery.append("?, ".repeat(tableColumnNames.size()));
        insertQuery = new StringBuilder(insertQuery.substring(0, insertQuery.length() - 2));
        insertQuery.append(")");
        return insertQuery;
    }

    /*
     * Access the database and execute the create
     * table query and insert data queries
     */
    private static void executeQueries(StringBuilder createTableQuery, StringBuilder insertQuery ,Queue<List<String>> data, List<String> dataTypes) {
        try(Connection conn = DriverManager.getConnection(url)){
                dropTableIfExists(conn);
                createTable(conn, createTableQuery.toString());
        } catch (SQLException s) {
            s.printStackTrace();
        }

        int threads = Runtime.getRuntime().availableProcessors();
        int dataPerThread = data.size() / threads;
        List<Queue<List<String>>> dataPerThreadList = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            dataPerThreadList.add(new ConcurrentLinkedDeque<>());
            for (int j = 0; j < dataPerThread; j++) {
                dataPerThreadList.get(i).add(data.poll());
            }
        }
        dataPerThreadList.get(threads - 1).addAll(data);

        for (int i = 0; i < threads; i++) {
            int dataCounter = i;
            new Thread(() -> {
                try(Connection conn = DriverManager.getConnection(url)){
                    insertData(conn, insertQuery.toString(), dataPerThreadList.get(dataCounter), dataTypes);
                } catch (SQLException s) {
                    s.printStackTrace();
                }
            }).start();
        }
   }

    private static void dropTableIfExists(Connection conn) {
        String dropTableQuery = "DROP TABLE IF EXISTS " + ReadFromCSV.TABLE_NAME;
        try(PreparedStatement ps = conn.prepareStatement(dropTableQuery)) {
            ps.executeUpdate();
        } catch (SQLException s) {
            s.printStackTrace();
        }
    }

    private static void createTable(Connection conn, String createTableQuery) {
        try (PreparedStatement statement = conn.prepareStatement(createTableQuery)) {
            statement.executeUpdate();
        } catch (SQLException s) {
            s.printStackTrace();
        }
    }

    private static void insertData(Connection conn, String insertQuery ,Queue<List<String>> data, List<String> dataTypes) {
        try (PreparedStatement statement = conn.prepareStatement(insertQuery)) {
            int size = data.size();
            for (int i = 0; i< size;i++) {
                for (int j = 0; j < Objects.requireNonNull(data.peek()).size(); j++) {
                    switch (dataTypes.get(j)) {
                        case "INTEGER" -> statement.setInt(j + 1, Integer.parseInt(data.peek().get(j)));
                        case "DECIMAL(36,18)" -> statement.setBigDecimal(j + 1, new BigDecimal(data.peek().get(j)));
                        case "DATE" -> statement.setDate(j + 1, dateParse(data.peek().get(j)));
                        default -> statement.setString(j + 1, data.peek().get(j));
                    }
                }
                data.poll();
                statement.addBatch();
                if(i%batchSize==0) statement.executeBatch();
            }
            statement.executeBatch();
        } catch (SQLException s) {
            s.printStackTrace();
        }
    }

    private static java.sql.Date dateParse(String date) {
        java.util.Date date1 = new Date();
        try{
            if(date.matches("\\d{1,2}-\\d{1,2}-\\d{4}")) {
                date1 = new SimpleDateFormat("MM-dd-yyyy").parse(date);
            }
            if(date.matches("\\d{4}-\\d{1,2}-\\d{1,2}")) {
                date1 = new SimpleDateFormat("yyyy-MM-dd").parse(date);

            }
            if(date.matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
                date1 = new SimpleDateFormat("MM/dd/yyyy").parse(date);
            }
            if(date.matches("\\d{4}/\\d{1,2}/\\d{1,2}")) {
                date1 = new SimpleDateFormat("yyyy/MM/dd").parse(date);
            }
        }catch(ParseException e){
            e.printStackTrace();
        }
        return new java.sql.Date(date1.getTime());
    }
}