package com.github.terentich.adjust.dataloader.db;

import com.github.terentich.adjust.dataloader.model.IgraData;
import com.github.terentich.adjust.dataloader.model.IgraHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class DbManager {
    private static final Logger log = LoggerFactory.getLogger(DbManager.class);

    private static final String INSERT_IGRA_DATA_SQL = "INSERT INTO igra_data (\n" +
                                                       "    id,\n" +
                                                       "    year,\n" +
                                                       "    month,\n" +
                                                       "    day,\n" +
                                                       "    hour,\n" +
                                                       "    reltime,\n" +
                                                       "    numlev,\n" +
                                                       "    p_src,\n" +
                                                       "    np_src,\n" +
                                                       "    lat,\n" +
                                                       "    lon,\n" +
                                                       "    lvltyp1,\n" +
                                                       "    lvltyp2,\n" +
                                                       "    etime,\n" +
                                                       "    press,\n" +
                                                       "    pflag,\n" +
                                                       "    gph,\n" +
                                                       "    zflag,\n" +
                                                       "    temp,\n" +
                                                       "    tflag,\n" +
                                                       "    rh,\n" +
                                                       "    dpdp,\n" +
                                                       "    wdir,\n" +
                                                       "    wspd\n" +
                                                       ")\n" +
                                                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final Properties dbProperties;
    private Connection connection;

    public DbManager(Properties dbProperties) {
        this.dbProperties = dbProperties;
    }

    public void createDatabase() {
        try {
            Statement statement = connection.createStatement();
            Path ddlFile = Paths.get(Objects.requireNonNull(getClass().getResource("/ddl.sql")).toURI());
            String ddlSql = Files
                    .lines(ddlFile, StandardCharsets.UTF_8)
                    .filter(line -> !line.trim().isBlank())
                    .collect(Collectors.joining("\n"));

            for (String sql : ddlSql.split(";")) {
                log.info("Executing DDL SQL: \n{}", sql);
                statement.execute(sql);
            }
        } catch (SQLException | URISyntaxException | IOException e) {
            log.error("Unable to create database", e);
        }
    }

    public void showQueryResults(String sql) {
        try {
            Statement statement = connection.createStatement();
            statement.executeQuery(sql);
            ResultSet rs = statement.getResultSet();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                StringJoiner message = new StringJoiner(",");

                for (int i = 1; i <= columnCount; i++) {
                    message.add(metaData.getColumnName(i) + "=" + rs.getString(i));
                }

                System.out.println(message);
            }
        } catch (SQLException e) {
            log.error("Unable to query results", e);
        }
    }

    public Connection createDatabaseConnection() throws SQLException, IOException {
        // performance improvement
        dbProperties.put("reWriteBatchedInserts", "true");
        connection = DriverManager.getConnection(dbProperties.getProperty("url"), dbProperties);
        return connection;
    }

    public int saveData(List<IgraData> igraData) throws SQLException {
        int recordsAmount = igraData
                .stream()
                .mapToInt(data -> data.getRecords().size())
                .sum();
        log.info("Saving batch data: rows = {}", recordsAmount);
        connection.setAutoCommit(false);

        PreparedStatement ps = connection.prepareStatement(INSERT_IGRA_DATA_SQL);
        igraData.forEach(data -> createIngraDataSql(ps, data));
        ps.executeBatch();

        connection.commit();
        return recordsAmount;
    }

    private void createIngraDataSql(PreparedStatement ps, IgraData data) {
        IgraHeader header = data.getHeader();

        data.getRecords()
                .forEach(record -> {
                    try {
                        ps.setString(1, header.id());
                        ps.setInt(2, header.year());
                        ps.setInt(3, header.month());
                        ps.setInt(4, header.day());
                        ps.setInt(5, header.hour());
                        ps.setInt(6, header.reltime());
                        ps.setInt(7, header.numlev());
                        ps.setString(8, header.psrc());
                        ps.setString(9, header.npsrc());
                        ps.setInt(10, header.lat());
                        ps.setInt(11, header.lon());

                        ps.setInt(12, record.lvltyp1());
                        ps.setInt(13, record.lvltyp2());
                        ps.setInt(14, record.etime());
                        ps.setInt(15, record.press());
                        ps.setString(16, record.pflag());
                        ps.setInt(17, record.gph());
                        ps.setString(18, record.zflag());
                        ps.setInt(19, record.temp());
                        ps.setString(20, record.tflag());
                        ps.setInt(21, record.rh());
                        ps.setInt(22, record.dpdp());
                        ps.setInt(23, record.wdir());
                        ps.setInt(24, record.wspd());

                        ps.addBatch();
                    } catch (SQLException e) {
                        log.error("Unable to save data in database", e);
                    }
                });
    }
}